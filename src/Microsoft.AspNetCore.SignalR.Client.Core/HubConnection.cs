// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Pipelines;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Protocols.Features;
using Microsoft.AspNetCore.SignalR.Internal;
using Microsoft.AspNetCore.SignalR.Internal.Formatters;
using Microsoft.AspNetCore.SignalR.Internal.Protocol;
using Microsoft.AspNetCore.Sockets.Client;
using Microsoft.AspNetCore.Sockets.Internal;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Microsoft.AspNetCore.SignalR.Client
{
    public partial class HubConnection
    {
        public static readonly TimeSpan DefaultServerTimeout = TimeSpan.FromSeconds(30); // Server ping rate is 15 sec, this is 2 times that.

        private readonly ILoggerFactory _loggerFactory;
        private readonly ILogger _logger;
        private readonly IHubProtocol _protocol;
        private readonly Func<IConnection> _connectionFactory;
        private readonly HubBinder _binder;

        // This lock protects the connection and the disposed flag to ensure changes to them are seen at the same time.
        private readonly SemaphoreSlim _startLock = new SemaphoreSlim(1, 1);
        private bool _disposed;
        private IConnection _connection;

        private readonly object _pendingCallsLock = new object();
        private readonly Dictionary<string, InvocationRequest> _pendingCalls = new Dictionary<string, InvocationRequest>();
        private readonly ConcurrentDictionary<string, List<InvocationHandler>> _handlers = new ConcurrentDictionary<string, List<InvocationHandler>>();
        private CancellationTokenSource _connectionActive;

        private int _nextId;
        private Task _receiveTask;

        public event Action<Exception> Closed;

        /// <summary>
        /// Gets or sets the server timeout interval for the connection. Changes to this value
        /// will not be applied until the Keep Alive timer is next reset.
        /// </summary>
        public TimeSpan ServerTimeout { get; set; } = DefaultServerTimeout;

        public HubConnection(Func<IConnection> connectionFactory, IHubProtocol protocol) : this(connectionFactory, protocol, NullLoggerFactory.Instance)
        {
        }

        public HubConnection(Func<IConnection> connectionFactory, IHubProtocol protocol, ILoggerFactory loggerFactory)
        {
            _connectionFactory = connectionFactory ?? throw new ArgumentNullException(nameof(connectionFactory));
            _protocol = protocol ?? throw new ArgumentNullException(nameof(protocol));

            _binder = new HubBinder(this);
            _loggerFactory = loggerFactory ?? NullLoggerFactory.Instance;
            _logger = _loggerFactory.CreateLogger<HubConnection>();
        }

        public async Task StartAsync()
        {
            CheckDisposed();
            await StartAsyncCore().ForceAsync();
        }

        public async Task StopAsync()
        {
            CheckDisposed();
            await StopAsyncCore().ForceAsync();
        }

        public async Task DisposeAsync()
        {
            if (!_disposed)
            {
                await DisposeAsyncCore().ForceAsync();
            }
        }

        public IDisposable On(string methodName, Type[] parameterTypes, Func<object[], object, Task> handler, object state)
        {
            CheckDisposed();

            // It's OK to be disposed while registering a callback, we'll just never call the callback anyway (as with all the callbacks registered before disposal).
            var invocationHandler = new InvocationHandler(parameterTypes, handler, state);
            var invocationList = _handlers.AddOrUpdate(methodName, _ => new List<InvocationHandler> { invocationHandler },
                (_, invocations) =>
                {
                    lock (invocations)
                    {
                        invocations.Add(invocationHandler);
                    }
                    return invocations;
                });

            return new Subscription(invocationHandler, invocationList);
        }

        public async Task<ChannelReader<object>> StreamAsChannelAsync(string methodName, Type returnType, object[] args, CancellationToken cancellationToken = default) =>
            await StreamAsChannelAsyncCore(methodName, returnType, args, cancellationToken).ForceAsync();

        public async Task<object> InvokeAsync(string methodName, Type returnType, object[] args, CancellationToken cancellationToken = default) =>
            await InvokeAsyncCore(methodName, returnType, args, cancellationToken).ForceAsync();

        public async Task SendAsync(string methodName, object[] args, CancellationToken cancellationToken = default) =>
            await SendAsyncCore(methodName, args, cancellationToken).ForceAsync();

        private async Task StartAsyncCore()
        {
            await _startLock.WaitAsync();
            try
            {
                if (_connection != null)
                {
                    // We're already connected
                    return;
                }

                // Check if we finished stopping (the check above proved that we were asked to stop)
                // (We're the only ones who set _receiveTask, so this is thread-safe, because we're in our lock)
                if (_receiveTask != null && !_receiveTask.IsCompleted)
                {
                    // Wait for the previous stop to finish.
                    await _receiveTask;
                }

                CheckDisposed();

                // Start the connection
                _connection = _connectionFactory();
                await _connection.StartAsync(_protocol.TransferFormat);

                Log.HubProtocol(_logger, _protocol.Name);

                await HandshakeAsync();

                // From here on, "ReceiveLoop" is in charge of the connection.
                _receiveTask = ReceiveLoop();
            }
            catch when (_connection != null)
            {
                // We can't catch exceptions thrown by RecieveLoop because we don't await it, so we know it hasn't started yet.

                // If we got partially started, we need to shut down the connection before we rethrow.
                await _connection.DisposeAsync();
                _connection = null;
                throw;
            }
            finally
            {
                _startLock.Release();
            }
        }

        private async Task StopAsyncCore()
        {
            // Block a Start from happening until we've finished resolving.
            await _startLock.WaitAsync();
            try
            {
                CheckDisposed();
                await StopAsyncCoreWithinLock();
            }
            finally
            {
                _startLock.Release();
            }
        }

        private async Task DisposeAsyncCore()
        {
            // Block a Start from happening until we've finished resolving.
            await _startLock.WaitAsync();
            try
            {
                if (_disposed)
                {
                    // We're already disposed
                    return;
                }

                // Stop the connection (if it's running)
                await StopAsyncCoreWithinLock();

                // Mark ourselves as disposed, so we can't be restarted.
                _disposed = true;
            }
            finally
            {
                _startLock.Release();
            }
        }

        private async Task<ChannelReader<object>> StreamAsChannelAsyncCore(string methodName, Type returnType, object[] args, CancellationToken cancellationToken)
        {
            CheckDisposed();
            await _startLock.WaitAsync(cancellationToken);
            try
            {
                CheckDisposed();
                if (_connection == null)
                {
                    throw new InvalidOperationException($"The '{nameof(StreamAsChannelAsync)}' method cannot be called if the connection is not active");
                }

                var irq = InvocationRequest.Stream(cancellationToken, returnType, GetNextId(), _loggerFactory, this, out var channel);
                await InvokeStreamCore(methodName, irq, args, cancellationToken);

                if (cancellationToken.CanBeCanceled)
                {
                    cancellationToken.Register(state =>
                    {
                        var invocationReq = (InvocationRequest)state;
                        if (!invocationReq.HubConnection._connectionActive.IsCancellationRequested)
                        {
                            // Fire and forget, if it fails that means we aren't connected anymore.
                            _ = invocationReq.HubConnection.SendHubMessage(new CancelInvocationMessage(invocationReq.InvocationId), cancellationToken: default);
                        }

                        // Cancel the invocation
                        invocationReq.Dispose();
                    }, irq);
                }

                return channel;
            }
            finally
            {
                _startLock.Release();
            }
        }

        private async Task<object> InvokeAsyncCore(string methodName, Type returnType, object[] args, CancellationToken cancellationToken)
        {
            CheckDisposed();
            await _startLock.WaitAsync(cancellationToken);

            Task<object> invocationTask;
            try
            {
                CheckDisposed();
                if (_connection == null)
                {
                    throw new InvalidOperationException($"The '{nameof(InvokeAsync)}' method cannot be called if the connection is not active");
                }

                var irq = InvocationRequest.Invoke(cancellationToken, returnType, GetNextId(), _loggerFactory, this, out invocationTask);
                await InvokeCore(methodName, irq, args, cancellationToken);
            }
            finally
            {
                _startLock.Release();
            }

            // Wait for this outside the lock, because it won't complete until the server responds.
            return await invocationTask;
        }

        private async Task InvokeCore(string methodName, InvocationRequest irq, object[] args, CancellationToken cancellationToken)
        {
            AssertConnectionValid();

            Log.PreparingBlockingInvocation(_logger, irq.InvocationId, methodName, irq.ResultType.FullName, args.Length);

            // Client invocations are always blocking
            var invocationMessage = new InvocationMessage(irq.InvocationId, target: methodName,
                argumentBindingException: null, arguments: args);

            Log.RegisterInvocation(_logger, invocationMessage.InvocationId);

            AddInvocation(irq);

            // Trace the full invocation
            Log.IssueInvocation(_logger, invocationMessage.InvocationId, irq.ResultType.FullName, methodName, args);

            try
            {
                await SendHubMessage(invocationMessage, cancellationToken);
            }
            catch (Exception ex)
            {
                Log.SendInvocationFailed(_logger, invocationMessage.InvocationId, ex);
                TryRemoveInvocation(invocationMessage.InvocationId, out _);

                // Rethrow the send failure.
                throw;
            }
        }

        private async Task InvokeStreamCore(string methodName, InvocationRequest irq, object[] args, CancellationToken cancellationToken)
        {
            AssertConnectionValid();

            Log.PreparingStreamingInvocation(_logger, irq.InvocationId, methodName, irq.ResultType.FullName, args.Length);

            var invocationMessage = new StreamInvocationMessage(irq.InvocationId, methodName,
                argumentBindingException: null, arguments: args);

            // I just want an excuse to use 'irq' as a variable name...
            Log.RegisterInvocation(_logger, invocationMessage.InvocationId);

            AddInvocation(irq);

            // Trace the full invocation
            Log.IssueInvocation(_logger, invocationMessage.InvocationId, irq.ResultType.FullName, methodName, args);

            try
            {
                await SendHubMessage(invocationMessage, cancellationToken);
            }
            catch (Exception ex)
            {
                Log.SendInvocationFailed(_logger, invocationMessage.InvocationId, ex);
                TryRemoveInvocation(invocationMessage.InvocationId, out _);
            }
        }

        private async Task SendHubMessage(HubInvocationMessage hubMessage, CancellationToken cancellationToken)
        {
            AssertConnectionValid();

            var payload = _protocol.WriteToArray(hubMessage);
            Log.SendInvocation(_logger, hubMessage.InvocationId);

            await WriteAsync(payload, cancellationToken);
            Log.SendInvocationCompleted(_logger, hubMessage.InvocationId);
        }

        private async Task SendAsyncCore(string methodName, object[] args, CancellationToken cancellationToken)
        {
            CheckDisposed();

            await _startLock.WaitAsync(cancellationToken);
            try
            {
                CheckDisposed();
                if (_connection == null)
                {
                    throw new InvalidOperationException($"The '{nameof(SendAsync)}' method cannot be called if the connection is not active");
                }

                var invocationMessage = new InvocationMessage(null, target: methodName,
                    argumentBindingException: null, arguments: args);

                await SendHubMessage(invocationMessage, cancellationToken);
            }
            finally
            {
                _startLock.Release();
            }
        }

        private async Task<(bool close, Exception exception)> ProcessMessagesAsync(ReadOnlySequence<byte> buffer)
        {
            // TODO: Don't ToArray it :)
            var data = buffer.ToArray();

            var currentData = new ReadOnlyMemory<byte>(data);
            Log.ParsingMessages(_logger, currentData.Length);

            var messages = new List<HubMessage>();
            if (_protocol.TryParseMessages(currentData, _binder, messages))
            {
                Log.ReceivingMessages(_logger, messages.Count);
                foreach (var message in messages)
                {
                    InvocationRequest irq;
                    switch (message)
                    {
                        case InvocationMessage invocation:
                            Log.ReceivedInvocation(_logger, invocation.InvocationId, invocation.Target,
                                invocation.ArgumentBindingException != null ? null : invocation.Arguments);
                            await DispatchInvocationAsync(invocation);
                            break;
                        case CompletionMessage completion:
                            if (!TryRemoveInvocation(completion.InvocationId, out irq))
                            {
                                Log.DropCompletionMessage(_logger, completion.InvocationId);
                            }
                            else
                            {
                                DispatchInvocationCompletion(completion, irq);
                                irq.Dispose();
                            }
                            break;
                        case StreamItemMessage streamItem:
                            // Complete the invocation with an error, we don't support streaming (yet)
                            if (!TryGetInvocation(streamItem.InvocationId, out irq))
                            {
                                Log.DropStreamMessage(_logger, streamItem.InvocationId);
                                return (close: false, exception: null);
                            }
                            DispatchInvocationStreamItemAsync(streamItem, irq);
                            break;
                        case CloseMessage close:
                            if (string.IsNullOrEmpty(close.Error))
                            {
                                Log.ReceivedClose(_logger);
                                return (close: true, exception: null);
                            }
                            else
                            {
                                Log.ReceivedCloseWithError(_logger, close.Error);
                                return (close: true, exception: new HubException($"The server closed the connection with the following error: {close.Error}"));
                            }
                        case PingMessage _:
                            Log.ReceivedPing(_logger);
                            // Nothing to do on receipt of a ping.
                            break;
                        default:
                            throw new InvalidOperationException($"Unexpected message type: {message.GetType().FullName}");
                    }
                }
                Log.ProcessedMessages(_logger, messages.Count);
            }
            else
            {
                Log.FailedParsing(_logger, data.Length);
            }

            return (close: false, exception: null);
        }

        private void CancelOutstandingInvocations(Exception exception)
        {
            lock (_pendingCallsLock)
            {
                // We cancel inside the lock to make sure everyone who was part-way through registering an invocation
                // completes. This also ensures that nobody will add things to _pendingCalls after we leave this block
                // because everything that adds to _pendingCalls checks _connectionActive first (inside the _pendingCallsLock)
                _connectionActive.Cancel();

                foreach (var outstandingCall in _pendingCalls.Values)
                {
                    Log.RemoveInvocation(_logger, outstandingCall.InvocationId);
                    if (exception != null)
                    {
                        outstandingCall.Fail(exception);
                    }
                    outstandingCall.Dispose();
                }
                _pendingCalls.Clear();
            }
        }

        private async Task DispatchInvocationAsync(InvocationMessage invocation)
        {
            // Find the handler
            if (!_handlers.TryGetValue(invocation.Target, out var handlers))
            {
                Log.MissingHandler(_logger, invocation.Target);
                return;
            }

            // TODO: Optimize this!
            // Copying the callbacks to avoid concurrency issues
            InvocationHandler[] copiedHandlers;
            lock (handlers)
            {
                copiedHandlers = new InvocationHandler[handlers.Count];
                handlers.CopyTo(copiedHandlers);
            }

            foreach (var handler in copiedHandlers)
            {
                try
                {
                    await handler.InvokeAsync(invocation.Arguments);
                }
                catch (Exception ex)
                {
                    Log.ErrorInvokingClientSideMethod(_logger, invocation.Target, ex);
                }
            }
        }

        // This async void is GROSS but we need to dispatch asynchronously because we're writing to a Channel
        // and there's nobody to actually wait for us to finish.
        private async void DispatchInvocationStreamItemAsync(StreamItemMessage streamItem, InvocationRequest irq)
        {
            Log.ReceivedStreamItem(_logger, streamItem.InvocationId);

            if (irq.CancellationToken.IsCancellationRequested)
            {
                Log.CancelingStreamItem(_logger, irq.InvocationId);
            }
            else if (!await irq.StreamItem(streamItem.Item))
            {
                Log.ReceivedStreamItemAfterClose(_logger, irq.InvocationId);
            }
        }

        private void DispatchInvocationCompletion(CompletionMessage completion, InvocationRequest irq)
        {
            Log.ReceivedInvocationCompletion(_logger, completion.InvocationId);

            if (irq.CancellationToken.IsCancellationRequested)
            {
                Log.CancelingInvocationCompletion(_logger, irq.InvocationId);
            }
            else
            {
                irq.Complete(completion);
            }
        }

        private string GetNextId() => Interlocked.Increment(ref _nextId).ToString();

        private void AddInvocation(InvocationRequest irq)
        {
            AssertConnectionValid();

            lock (_pendingCallsLock)
            {
                if (_pendingCalls.ContainsKey(irq.InvocationId))
                {
                    Log.InvocationAlreadyInUse(_logger, irq.InvocationId);
                    throw new InvalidOperationException($"Invocation ID '{irq.InvocationId}' is already in use.");
                }
                else
                {
                    _pendingCalls.Add(irq.InvocationId, irq);
                }
            }
        }

        private bool TryGetInvocation(string invocationId, out InvocationRequest irq)
        {
            lock (_pendingCallsLock)
            {
                return _pendingCalls.TryGetValue(invocationId, out irq);
            }
        }

        private bool TryRemoveInvocation(string invocationId, out InvocationRequest irq)
        {
            lock (_pendingCallsLock)
            {
                if (_pendingCalls.TryGetValue(invocationId, out irq))
                {
                    _pendingCalls.Remove(invocationId);
                    return true;
                }
                else
                {
                    return false;
                }
            }
        }

        private void CheckDisposed()
        {
            if (_disposed)
            {
                throw new ObjectDisposedException(nameof(HubConnection));
            }
        }

        private async Task HandshakeAsync()
        {
            // Send the Handshake request
            _connectionActive = new CancellationTokenSource();
            using (var memoryStream = new MemoryStream())
            {
                Log.SendingHubHandshake(_logger);
                HandshakeProtocol.WriteRequestMessage(new HandshakeRequestMessage(_protocol.Name), memoryStream);
                var result = await WriteAsync(memoryStream.ToArray(), _connectionActive.Token);
                if (result.IsCompleted)
                {
                    // The other side disconnected
                    throw new InvalidOperationException("The server disconnected before the handshake was completed");
                }
            }

            while (true)
            {
                var result = await _connection.Transport.Input.ReadAsync();
                var buffer = result.Buffer;
                var consumed = buffer.Start;
                var examined = buffer.End;

                try
                {
                    // read first message out of the incoming data
                    if (TextMessageParser.TryParseMessage(ref buffer, out var payload))
                    {
                        consumed = buffer.Start;
                        var message = HandshakeProtocol.ParseResponseMessage(payload.ToArray());

                        if (!string.IsNullOrEmpty(message.Error))
                        {
                            throw new HubException(
                                $"Unable to complete handshake with the server due to an error: {message.Error}");
                        }
                    }
                    else if (result.IsCompleted)
                    {
                        // Not enough data, and we won't be getting any more data.
                        throw new InvalidOperationException(
                            "The server disconnected before sending a handshake response");
                    }
                }
                catch (Exception ex)
                {
                    // shutdown if we're unable to read handshake
                    Log.ErrorReceivingHandshakeResponse(_logger, ex);
                    throw;
                }
                finally
                {
                    _connection.Transport.Input.AdvanceTo(consumed, examined);
                }
            }
        }

        private async Task ReceiveLoop()
        {
            // Check if we need keep-alive
            Timer timeoutTimer = null;
            if (_connection.Features.Get<IConnectionInherentKeepAliveFeature>() == null)
            {
                timeoutTimer = new Timer(state => ((IConnection)state).Transport.Input.CancelPendingRead(), _connection,
                    ServerTimeout, Timeout.InfiniteTimeSpan);
            }

            Exception closeException = null;

            try
            {
                while (true)
                {
                    var result = await _connection.Transport.Input.ReadAsync();
                    var buffer = result.Buffer;
                    var consumed = buffer.End; // TODO: Support partial messages
                    var examined = buffer.End;

                    try
                    {

                        if (buffer.IsEmpty && result.IsCompleted)
                        {
                            // The server disconnected
                            break;
                        }
                        else if (result.IsCanceled)
                        {
                            // We aborted because the server timed-out
                            closeException = new TimeoutException($"Server timeout ({ServerTimeout.TotalMilliseconds:0.00}ms) elapsed without receiving a message from the server.");
                            break;
                        }

                        timeoutTimer?.Change(ServerTimeout, Timeout.InfiniteTimeSpan);

                        // We have data, process it
                        var (close, exception) = await ProcessMessagesAsync(buffer);
                        if (close)
                        {
                            // Closing because we got a close frame, possibly with an error in it.
                            closeException = exception;
                            break;
                        }
                    }
                    finally
                    {
                        _connection.Transport.Input.AdvanceTo(consumed, examined);
                    }
                }
            }
            finally
            {
                // Dispose the connection
                await _connection.DisposeAsync();
                _connection = null;

                // Stop the timeout timer.
                timeoutTimer?.Dispose();
            }

            // Cancel any outstanding invocations
            CancelOutstandingInvocations(closeException);

            // Fire-and-forget the closed event
            _ = Task.Run(() =>
            {
                try
                {
                    Closed?.Invoke(closeException);
                }
                catch (Exception ex)
                {
                    Log.ErrorDuringClosedEvent(_logger, ex);
                }
            });
        }

        private ValueTask<FlushResult> WriteAsync(byte[] payload, CancellationToken token) =>
            _connection.Transport.Output.WriteAsync(payload, token);

        private async Task StopAsyncCoreWithinLock()
        {
            if (_connection == null)
            {
                // No-op if we're already stopped.
                return;
            }

            // Complete our write pipe, which should cause everything to shut down
            _connection.Transport.Output.Complete();

            // Wait ServerTimeout for the server or transport to shut down.
            var delayCts = new CancellationTokenSource();
            var trigger = await Task.WhenAny(_receiveTask, Task.Delay(ServerTimeout, delayCts.Token));
            if (trigger == _receiveTask)
            {
                // Stop the timer.
                delayCts.Cancel();
            }
            else
            {
                // We timed out waiting for the server/transport to shut down, cancel it, this will cause
                // a TimeoutException indicating the server timeout elapsed to be signalled as
                // the exception in the Closed event.
                _connection.Transport.Input.CancelPendingRead();
                await _receiveTask;
            }

            _receiveTask = null;
        }

        // Debug.Assert plays havoc with Unit Tests. But I want something that I can "assert" only in Debug builds.
        [Conditional("DEBUG")]
        private static void SafeAssert(bool condition, string message, [CallerMemberName] string memberName = null, [CallerFilePath] string fileName = null, [CallerLineNumber] int lineNumber = 0)
        {
            if (!condition)
            {
                throw new Exception($"Assertion failed in {memberName}, at {fileName}:{lineNumber}: {message}");
            }
        }

        [Conditional("DEBUG")]
        private void AssertInConnectionLock() => SafeAssert(_startLock.CurrentCount == 0, "We're not in the Connection Lock!");

        [Conditional("DEBUG")]
        private void AssertConnectionValid()
        {
            AssertInConnectionLock();
            SafeAssert(_connection != null, "We don't have a connection!");
        }

        private class Subscription : IDisposable
        {
            private readonly InvocationHandler _handler;
            private readonly List<InvocationHandler> _handlerList;

            public Subscription(InvocationHandler handler, List<InvocationHandler> handlerList)
            {
                _handler = handler;
                _handlerList = handlerList;
            }

            public void Dispose()
            {
                lock (_handlerList)
                {
                    _handlerList.Remove(_handler);
                }
            }
        }

        private class HubBinder : IInvocationBinder
        {
            private readonly HubConnection _connection;

            public HubBinder(HubConnection connection)
            {
                _connection = connection;
            }

            public Type GetReturnType(string invocationId)
            {
                if (!_connection._pendingCalls.TryGetValue(invocationId, out var irq))
                {
                    Log.ReceivedUnexpectedResponse(_connection._logger, invocationId);
                    return null;
                }
                return irq.ResultType;
            }

            public IReadOnlyList<Type> GetParameterTypes(string methodName)
            {
                if (!_connection._handlers.TryGetValue(methodName, out var handlers))
                {
                    Log.MissingHandler(_connection._logger, methodName);
                    return Type.EmptyTypes;
                }

                // We use the parameter types of the first handler
                lock (handlers)
                {
                    if (handlers.Count > 0)
                    {
                        return handlers[0].ParameterTypes;
                    }
                    throw new InvalidOperationException($"There are no callbacks registered for the method '{methodName}'");
                }
            }
        }

        private struct InvocationHandler
        {
            public Type[] ParameterTypes { get; }
            private readonly Func<object[], object, Task> _callback;
            private readonly object _state;

            public InvocationHandler(Type[] parameterTypes, Func<object[], object, Task> callback, object state)
            {
                _callback = callback;
                ParameterTypes = parameterTypes;
                _state = state;
            }

            public Task InvokeAsync(object[] parameters)
            {
                return _callback(parameters, _state);
            }
        }
    }
}
