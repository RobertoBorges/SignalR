// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Diagnostics;
using System.IO;
using System.IO.Pipelines;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Features;
using Microsoft.AspNetCore.Protocols;
using Microsoft.AspNetCore.Protocols.Features;
using Microsoft.AspNetCore.Sockets.Internal;
using Microsoft.AspNetCore.Sockets.Internal.Transports;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Primitives;
using Newtonsoft.Json;

namespace Microsoft.AspNetCore.Sockets
{
    public class LegacyHttpConnectionDispatcher : HttpConnectionDispatcher
    {
        public LegacyHttpConnectionDispatcher(ConnectionManager manager, ILoggerFactory loggerFactory) : base(manager, loggerFactory)
        {
        }

        protected override string GetConnectionId(HttpContext context) => context.Request.Query["connectionToken"];

        public async Task ExecuteNegotiateAsync(HttpContext context, HttpSocketOptions options, ConnectionDelegate connectionDelegate)
        {
            var logScope = new ConnectionLogScope(GetConnectionId(context));
            using (_logger.BeginScope(logScope))
            {
                if (!await AuthorizeHelper.AuthorizeAsync(context, options.AuthorizationData))
                {
                    return;
                }

                await ProcessNegotiate(context, options, logScope);
            }
        }

        protected override async Task<bool> EnsureConnectionStateAsync(DefaultConnectionContext connection, HttpContext context, TransportType transportType, TransportType supportedTransports, ConnectionLogScope logScope, HttpSocketOptions options)
        {
            if (!await base.EnsureConnectionStateAsync(connection, context, transportType, supportedTransports, logScope, options))
            {
                return false;
            }

            var connectionData = context.Request.Query["connectionData"];

            connection.Items["connectionData"] = connectionData;

            return true;
        }

        public async Task ExecuteConnectAsync(HttpContext context, HttpSocketOptions options, ConnectionDelegate connectionDelegate)
        {
            var logScope = new ConnectionLogScope(GetConnectionId(context));
            using (_logger.BeginScope(logScope))
            {
                if (!await AuthorizeHelper.AuthorizeAsync(context, options.AuthorizationData))
                {
                    return;
                }

                var transport = context.Request.Query["transport"];

                if (string.Equals(transport, "websockets", StringComparison.OrdinalIgnoreCase))
                {
                    await ExecuteWebSockets(context, connectionDelegate, options, logScope);
                }
                else if (string.Equals(transport, "serverSentEvents", StringComparison.OrdinalIgnoreCase))
                {
                    await ExecuteServerSentEvents(context, connectionDelegate, options, logScope);
                }
                else if (string.Equals(transport, "longPolling", StringComparison.OrdinalIgnoreCase))
                {
                    await ExecuteLongPolling(context, connectionDelegate, options, logScope);
                }

                // No forever frame...
            }
        }

        public async Task ExecuteAbortAsync(HttpContext context, HttpSocketOptions options, ConnectionDelegate connectionDelegate)
        {
            var connection = await GetConnectionAsync(context, options);
            if (connection == null)
            {
                // No such connection, GetConnection already set the response status code
                return;
            }

            // Stop the transport (doesn't work for long polling yet)
            connection.GetHttpContext().Abort();

            // Should work for long polling
            connection.Cancellation?.Cancel();
        }

        public Task ExecuteStartAsync(HttpContext context, HttpSocketOptions options, ConnectionDelegate connectionDelegate)
        {
            return context.Response.WriteAsync("{\"Response\":\"started\"}");
        }

        public Task ExecutePingAsync(HttpContext context, HttpSocketOptions options, ConnectionDelegate connectionDelegate)
        {
            return context.Response.WriteAsync("{\"Response\":\"pong\"}");
        }

        public Task ExecuteReconnectAsync(HttpContext context, HttpSocketOptions options, ConnectionDelegate connectionDelegate)
        {
            return Task.CompletedTask;
        }

        public async Task ExecuteSendAsync(HttpContext context, HttpSocketOptions options, ConnectionDelegate connectionDelegate)
        {
            if (!context.Request.HasFormContentType)
            {
                // Some clients don't set this
                context.Request.ContentType = "application/x-www-form-urlencoded; charset=utf-8";
            }

            var connection = await GetConnectionAsync(context, options);
            if (connection == null)
            {
                // No such connection, GetConnection already set the response status code
                return;
            }

            context.Response.ContentType = "text/plain";

            var transport = (TransportType?)connection.Items[ConnectionMetadataNames.Transport];
            if (transport == TransportType.WebSockets)
            {
                context.Response.StatusCode = StatusCodes.Status405MethodNotAllowed;
                await context.Response.WriteAsync("POST requests are not allowed for WebSocket connections.");
                return;
            }

            var form = await context.Request.ReadFormAsync();
            var data = form["data"];

            if (string.IsNullOrEmpty(data))
            {
                return;
            }

            await connection.Application.Output.WriteAsync(Encoding.UTF8.GetBytes(data));
        }

        private Task ProcessNegotiate(HttpContext context, HttpSocketOptions options, ConnectionLogScope logScope)
        {
            context.Response.ContentType = "application/json";

            // Establish the connection
            var connection = _manager.CreateConnection();

            // Set the Connection ID on the logging scope so that logs from now on will have the
            // Connection ID metadata set.
            logScope.ConnectionId = connection.ConnectionId;

            // Get the bytes for the connection id
            var negotiateResponseBuffer = Encoding.UTF8.GetBytes(GetNegotiatePayload(connection.ConnectionId, context, options));

            // Write it out to the response with the right content length
            context.Response.ContentLength = negotiateResponseBuffer.Length;
            return context.Response.Body.WriteAsync(negotiateResponseBuffer, 0, negotiateResponseBuffer.Length);
        }

        private static string GetNegotiatePayload(string connectionId, HttpContext context, HttpSocketOptions options)
        {
            var sb = new StringBuilder();
            using (var jsonWriter = new JsonTextWriter(new StringWriter(sb)))
            {
                jsonWriter.WriteStartObject();
                jsonWriter.WritePropertyName("ConnectionToken");
                jsonWriter.WriteValue(connectionId);
                jsonWriter.WritePropertyName("ConnectionId");
                jsonWriter.WriteValue(connectionId);
                jsonWriter.WritePropertyName("KeepAliveTimeout");
                jsonWriter.WriteValue("100000.0");
                jsonWriter.WritePropertyName("DisconnectTimeout");
                jsonWriter.WriteValue("5.0");
                jsonWriter.WritePropertyName("TryWebSockets");
                jsonWriter.WriteValue(ServerHasWebSockets(context.Features).ToString());
                jsonWriter.WritePropertyName("ProtocolVersion");
                jsonWriter.WriteValue(context.Request.Query["clientProtocol"]);
                jsonWriter.WritePropertyName("TransportConnectTimeout");
                jsonWriter.WriteValue("30");
                jsonWriter.WritePropertyName("LongPollDelay");
                jsonWriter.WriteValue("0.0");
                jsonWriter.WriteEndObject();
            }

            return sb.ToString();
        }

        private static bool ServerHasWebSockets(IFeatureCollection features)
        {
            return features.Get<IHttpWebSocketFeature>() != null;
        }
    }
}
