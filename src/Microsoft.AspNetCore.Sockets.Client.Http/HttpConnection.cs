// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.IO;
using System.IO.Pipelines;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http.Features;
using Microsoft.AspNetCore.Protocols;
using Microsoft.AspNetCore.Sockets.Client.Http;
using Microsoft.AspNetCore.Sockets.Http.Internal;
using Microsoft.AspNetCore.Sockets.Internal;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Newtonsoft.Json;

namespace Microsoft.AspNetCore.Sockets.Client
{
    public partial class HttpConnection : IConnection
    {
        private static readonly TimeSpan HttpClientTimeout = TimeSpan.FromSeconds(120);

        private readonly ILoggerFactory _loggerFactory;
        private readonly ILogger _logger;

        private int _state = (int)ConnectionState.Disconnected;
        private TaskCompletionSource<object> _startTcs = new TaskCompletionSource<object>();
        private TaskCompletionSource<object> _disposeTcs = new TaskCompletionSource<object>();

        private IDuplexPipe _transportPipe;
        private IDuplexPipe _applicationPipe;

        private readonly HttpClient _httpClient;
        private readonly HttpOptions _httpOptions;
        private ITransport _transport;
        private readonly ITransportFactory _transportFactory;
        private string _connectionId;
        private readonly TransportType _requestedTransportType = TransportType.All;
        private readonly ConnectionLogScope _logScope;
        private readonly IDisposable _scopeDisposable;

        public Uri Url { get; }

        public IDuplexPipe Transport
        {
            get
            {
                if (_transportPipe == null)
                {
                    throw new InvalidOperationException($"Cannot access the {nameof(Transport)} pipe before the connection has started");
                }
                return _transportPipe;
            }
        }

        public IFeatureCollection Features { get; } = new FeatureCollection();

        public HttpConnection(Uri url)
            : this(url, TransportType.All)
        { }

        public HttpConnection(Uri url, TransportType transportType)
            : this(url, transportType, loggerFactory: null)
        {
        }

        public HttpConnection(Uri url, ILoggerFactory loggerFactory)
            : this(url, TransportType.All, loggerFactory, httpOptions: null)
        {
        }

        public HttpConnection(Uri url, TransportType transportType, ILoggerFactory loggerFactory)
            : this(url, transportType, loggerFactory, httpOptions: null)
        {
        }

        public HttpConnection(Uri url, TransportType transportType, ILoggerFactory loggerFactory, HttpOptions httpOptions)
        {
            Url = url ?? throw new ArgumentNullException(nameof(url));

            _loggerFactory = loggerFactory ?? NullLoggerFactory.Instance;
            _logger = _loggerFactory.CreateLogger<HttpConnection>();
            _httpOptions = httpOptions;

            _requestedTransportType = transportType;
            if (_requestedTransportType != TransportType.WebSockets)
            {
                _httpClient = CreateHttpClient();
            }

            _transportFactory = new DefaultTransportFactory(transportType, _loggerFactory, _httpClient, httpOptions);
            _logScope = new ConnectionLogScope();
            _scopeDisposable = _logger.BeginScope(_logScope);
        }

        public HttpConnection(Uri url, ITransportFactory transportFactory, ILoggerFactory loggerFactory, HttpOptions httpOptions)
        {
            Url = url ?? throw new ArgumentNullException(nameof(url));
            _loggerFactory = loggerFactory ?? NullLoggerFactory.Instance;
            _logger = _loggerFactory.CreateLogger<HttpConnection>();
            _httpOptions = httpOptions;
            _httpClient = CreateHttpClient();
            _transportFactory = transportFactory ?? throw new ArgumentNullException(nameof(transportFactory));
            _logScope = new ConnectionLogScope();
            _scopeDisposable = _logger.BeginScope(_logScope);
        }

        public Task StartAsync() => StartAsync(TransferFormat.Binary);

        public async Task StartAsync(TransferFormat transferFormat)
        {
            await StartAsyncCore(transferFormat).ForceAsync();
        }

        private async Task StartAsyncCore(TransferFormat transferFormat)
        {
            var oldState = ChangeState(from: ConnectionState.Disconnected, to: ConnectionState.Starting);
            CheckDisposed(oldState);
            if (oldState != ConnectionState.Disconnected)
            {
                // We were "Started" or "Starting", just wait for the other start to finish
                await _startTcs.Task;
            }
            else
            {
                CheckDisposed();

                Log.HttpConnectionStarting(_logger);

                await SelectAndStartTransport(transferFormat);

                // Unblock any concurrent StartAsync operations waiting for us to finish.
                _startTcs.TrySetResult(null);
            }
        }

        public async Task DisposeAsync() => await DisposeAsyncCore().ForceAsync();

        private async Task DisposeAsyncCore()
        {
            if (ChangeState(to: ConnectionState.Disposed) == ConnectionState.Disposed)
            {
                // We were already disposed. Just return the pending Dispose (if any) task.
                await _disposeTcs.Task;
                return;
            }

            // Wait for any pending start to finish
            await _startTcs.Task;

            Log.DisposingClient(_logger);

            // Complete our ends of the pipes.
            _transportPipe.Input.Complete();
            _transportPipe.Output.Complete();

            // Wait for the transport to complete its side of the pipes
            await _transportPipe.Input.WaitForWriterToComplete();
            await _transportPipe.Output.WaitForReaderToComplete();

            _disposeTcs.TrySetResult(null);
        }

        private async Task SelectAndStartTransport(TransferFormat transferFormat)
        {
            if (_requestedTransportType == TransportType.WebSockets)
            {
                Log.StartingTransport(_logger, _requestedTransportType, Url);
                await StartTransport(Url, _requestedTransportType, transferFormat);
            }
            else
            {
                var negotiationResponse = await GetNegotiationResponse();

                // If we get disposed while starting, stop ASAP.
                CheckDisposed();

                // This should only need to happen once
                var connectUrl = CreateConnectUrl(Url, negotiationResponse.ConnectionId);

                // We're going to search for the transfer format as a string because we don't want to parse
                // all the transfer formats in the negotiation response, and we want to allow transfer formats
                // we don't understand in the negotiate response.
                var transferFormatString = transferFormat.ToString();

                foreach (var transport in negotiationResponse.AvailableTransports)
                {
                    // If we get disposed while starting, stop ASAP.
                    CheckDisposed();

                    if (!Enum.TryParse<TransportType>(transport.Transport, out var transportType))
                    {
                        Log.TransportNotSupported(_logger, transport.Transport);
                        continue;
                    }

                    try
                    {
                        if ((transportType & _requestedTransportType) == 0)
                        {
                            Log.TransportDisabledByClient(_logger, transportType);
                        }
                        else if (!transport.TransferFormats.Contains(transferFormatString, StringComparer.Ordinal))
                        {
                            Log.TransportDoesNotSupportTransferFormat(_logger, transportType, transferFormat);
                        }
                        else
                        {
                            // The negotiation response gets cleared in the fallback scenario.
                            if (negotiationResponse == null)
                            {
                                negotiationResponse = await GetNegotiationResponse();
                                connectUrl = CreateConnectUrl(Url, negotiationResponse.ConnectionId);
                            }

                            Log.StartingTransport(_logger, transportType, connectUrl);
                            await StartTransport(connectUrl, transportType, transferFormat);
                            break;
                        }
                    }
                    catch (Exception ex)
                    {
                        Log.TransportFailed(_logger, transportType, ex);
                        // Try the next transport
                        // Clear the negotiation response so we know to re-negotiate.
                        negotiationResponse = null;
                    }
                }
            }

            if (_transport == null)
            {
                throw new InvalidOperationException("Unable to connect to the server with any of the available transports.");
            }
        }

        private async Task<NegotiationResponse> Negotiate(Uri url, HttpClient httpClient, ILogger logger)
        {
            try
            {
                // Get a connection ID from the server
                Log.EstablishingConnection(logger, url);
                var urlBuilder = new UriBuilder(url);
                if (!urlBuilder.Path.EndsWith("/"))
                {
                    urlBuilder.Path += "/";
                }
                urlBuilder.Path += "negotiate";

                using (var request = new HttpRequestMessage(HttpMethod.Post, urlBuilder.Uri))
                {
                    // Corefx changed the default version and High Sierra curlhandler tries to upgrade request
                    request.Version = new Version(1, 1);
                    SendUtils.PrepareHttpRequest(request, _httpOptions);

                    using (var response = await httpClient.SendAsync(request))
                    {
                        response.EnsureSuccessStatusCode();
                        return await ParseNegotiateResponse(response, logger);
                    }
                }
            }
            catch (Exception ex)
            {
                Log.ErrorWithNegotiation(logger, url, ex);
                throw;
            }
        }

        private static async Task<NegotiationResponse> ParseNegotiateResponse(HttpResponseMessage response, ILogger logger)
        {
            NegotiationResponse negotiationResponse;
            using (var reader = new JsonTextReader(new StreamReader(await response.Content.ReadAsStreamAsync())))
            {
                try
                {
                    negotiationResponse = new JsonSerializer().Deserialize<NegotiationResponse>(reader);
                }
                catch (Exception ex)
                {
                    throw new FormatException("Invalid negotiation response received.", ex);
                }
            }

            if (negotiationResponse == null)
            {
                throw new FormatException("Invalid negotiation response received.");
            }

            return negotiationResponse;
        }

        private static Uri CreateConnectUrl(Uri url, string connectionId)
        {
            if (string.IsNullOrWhiteSpace(connectionId))
            {
                throw new FormatException("Invalid connection id.");
            }

            return Utils.AppendQueryString(url, "id=" + connectionId);
        }

        private async Task StartTransport(Uri connectUrl, TransportType transportType, TransferFormat transferFormat)
        {
            // Create the pipe pair (Application's writer is connected to Transport's reader, and vice versa)
            var options = new PipeOptions(writerScheduler: PipeScheduler.Inline, readerScheduler: PipeScheduler.ThreadPool, useSynchronizationContext: false);
            var pair = DuplexPipe.CreateConnectionPair(options, options);
            _transportPipe = pair.Transport;
            _applicationPipe = pair.Application;

            // Construct the transport
            _transport = _transportFactory.CreateTransport(transportType);

            // Start the transport, giving it one end of the pipe
            try
            {
                await _transport.StartAsync(connectUrl, pair.Application, transferFormat, this);
            }
            catch (Exception ex)
            {
                Log.ErrorStartingTransport(_logger, _transport, ex);
                _transport = null;
                throw;
            }
        }

        private HttpClient CreateHttpClient()
        {
            HttpMessageHandler httpMessageHandler = null;
            if (_httpOptions != null)
            {
                var httpClientHandler = new HttpClientHandler();
                if (_httpOptions.Proxy != null)
                {
                    httpClientHandler.Proxy = _httpOptions.Proxy;
                }
                if (_httpOptions.Cookies != null)
                {
                    httpClientHandler.CookieContainer = _httpOptions.Cookies;
                }
                if (_httpOptions.ClientCertificates != null)
                {
                    httpClientHandler.ClientCertificates.AddRange(_httpOptions.ClientCertificates);
                }
                if (_httpOptions.UseDefaultCredentials != null)
                {
                    httpClientHandler.UseDefaultCredentials = _httpOptions.UseDefaultCredentials.Value;
                }
                if (_httpOptions.Credentials != null)
                {
                    httpClientHandler.Credentials = _httpOptions.Credentials;
                }

                httpMessageHandler = httpClientHandler;
                if (_httpOptions.HttpMessageHandler != null)
                {
                    httpMessageHandler = _httpOptions.HttpMessageHandler(httpClientHandler);
                    if (httpMessageHandler == null)
                    {
                        throw new InvalidOperationException("Configured HttpMessageHandler did not return a value.");
                    }
                }
            }

            var httpClient = httpMessageHandler == null ? new HttpClient() : new HttpClient(httpMessageHandler);
            httpClient.Timeout = HttpClientTimeout;

            return httpClient;
        }

        private ConnectionState ChangeState(ConnectionState to) =>
            (ConnectionState)Interlocked.Exchange(ref _state, (int)to);

        private ConnectionState ChangeState(ConnectionState from, ConnectionState to) =>
            (ConnectionState)Interlocked.CompareExchange(ref _state, (int)to, (int)from);

        private void CheckDisposed() => CheckDisposed((ConnectionState)_state);
        private void CheckDisposed(ConnectionState state)
        {
            if (state == ConnectionState.Disposed)
            {
                throw new ObjectDisposedException(nameof(HttpConnection));
            }
        }

        private async Task<NegotiationResponse> GetNegotiationResponse()
        {
            var negotiationResponse = await Negotiate(Url, _httpClient, _logger);
            _connectionId = negotiationResponse.ConnectionId;
            _logScope.ConnectionId = _connectionId;
            return negotiationResponse;
        }

        private enum ConnectionState
        {
            Disconnected,
            Starting,
            Started,
            Disposed
        }

        private class NegotiationResponse
        {
            public string ConnectionId { get; set; }
            public AvailableTransport[] AvailableTransports { get; set; }
        }

        private class AvailableTransport
        {
            public string Transport { get; set; }
            public string[] TransferFormats { get; set; }
        }
    }
}
