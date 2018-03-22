// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Reflection;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Protocols;
using Microsoft.AspNetCore.Routing;

namespace Microsoft.AspNetCore.Sockets
{
    public class SocketRouteBuilder
    {
        private readonly HttpConnectionDispatcher _dispatcher;
        private readonly LegacyHttpConnectionDispatcher _legacyHttpConnectionDispatcher;
        private readonly RouteBuilder _routes;

        public SocketRouteBuilder(RouteBuilder routes, HttpConnectionDispatcher dispatcher, LegacyHttpConnectionDispatcher legacyHttpConnectionDispatcher)
        {
            _routes = routes;
            _dispatcher = dispatcher;
            _legacyHttpConnectionDispatcher = legacyHttpConnectionDispatcher;
        }

        public void MapSocket(string path, Action<IConnectionBuilder> socketConfig) =>
            MapSocket(new PathString(path), new HttpSocketOptions(), socketConfig);

        public void MapSocket(PathString path, Action<IConnectionBuilder> socketConfig) =>
            MapSocket(path, new HttpSocketOptions(), socketConfig);

        public void MapSocket(PathString path, HttpSocketOptions options, Action<IConnectionBuilder> connectionConfig)
        {
            var connectionBuilder = new ConnectionBuilder(_routes.ServiceProvider);
            connectionConfig(connectionBuilder);
            var socket = connectionBuilder.Build();
            _routes.MapRoute(path, c => _dispatcher.ExecuteAsync(c, options, socket));
            _routes.MapRoute(path + "/negotiate", c => _dispatcher.ExecuteNegotiateAsync(c, options));
        }

        public void MapLegacySocket(PathString path, HttpSocketOptions options, Action<IConnectionBuilder> connectionConfig)
        {
            var connectionBuilder = new ConnectionBuilder(_routes.ServiceProvider);
            connectionConfig(connectionBuilder);
            var socket = connectionBuilder.Build();
            // _routes.MapRoute(path, c => _legacyHttpConnectionDispatcher.ExecuteAsync(c, options, socket));
            _routes.MapRoute(path + "/negotiate", c => _legacyHttpConnectionDispatcher.ExecuteNegotiateAsync(c, options, socket));
            _routes.MapRoute(path + "/abort", c => _legacyHttpConnectionDispatcher.ExecuteAbortAsync(c, options, socket));
            _routes.MapRoute(path + "/connect", c => _legacyHttpConnectionDispatcher.ExecuteConnectAsync(c, options, socket));
            _routes.MapRoute(path + "/send", c => _legacyHttpConnectionDispatcher.ExecuteSendAsync(c, options, socket));
            _routes.MapRoute(path + "/start", c => _legacyHttpConnectionDispatcher.ExecuteStartAsync(c, options, socket));
            _routes.MapRoute(path + "/reconnect", c => _legacyHttpConnectionDispatcher.ExecuteReconnectAsync(c, options, socket));
            _routes.MapRoute(path + "/ping", c => _legacyHttpConnectionDispatcher.ExecutePingAsync(c, options, socket));
        }

        public void MapEndPoint<TEndPoint>(string path) where TEndPoint : EndPoint
        {
            MapEndPoint<TEndPoint>(new PathString(path), socketOptions: null);
        }

        public void MapEndPoint<TEndPoint>(PathString path) where TEndPoint : EndPoint
        {
            MapEndPoint<TEndPoint>(path, socketOptions: null);
        }

        public void MapEndPoint<TEndPoint>(PathString path, Action<HttpSocketOptions> socketOptions) where TEndPoint : EndPoint
        {
            var authorizeAttributes = typeof(TEndPoint).GetCustomAttributes<AuthorizeAttribute>(inherit: true);
            var options = new HttpSocketOptions();
            foreach (var attribute in authorizeAttributes)
            {
                options.AuthorizationData.Add(attribute);
            }
            socketOptions?.Invoke(options);

            MapSocket(path, options, builder =>
            {
                builder.UseEndPoint<TEndPoint>();
            });
        }
    }
}
