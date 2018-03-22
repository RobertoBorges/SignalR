// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Runtime.ExceptionServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Protocols;
using Microsoft.AspNetCore.Protocols.Features;
using Microsoft.AspNetCore.SignalR;
using Microsoft.AspNetCore.SignalR.Internal;
using Microsoft.AspNetCore.SignalR.Internal.Protocol;
using Microsoft.AspNetCore.Sockets;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using MsgPack.Serialization;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using SocketsSample.EndPoints;
using SocketsSample.Hubs;
using StackExchange.Redis;

namespace SocketsSample
{
    public class Startup
    {
        // This method gets called by the runtime. Use this method to add services to the container.
        // For more information on how to configure your application, visit http://go.microsoft.com/fwlink/?LinkID=398940
        public void ConfigureServices(IServiceCollection services)
        {
            services.AddSockets();

            services.AddSignalR(options =>
            {
                // Faster pings for testing
                options.KeepAliveInterval = TimeSpan.FromSeconds(5);
            })
            .AddMessagePackProtocol(options =>
            {
                options.SerializationContext.DictionarySerlaizationOptions.KeyTransformer = DictionaryKeyTransformers.LowerCamel;
            });
            // .AddRedis();

            services.AddCors(o =>
            {
                o.AddPolicy("Everything", p =>
                {
                    p.AllowAnyHeader()
                     .AllowAnyMethod()
                     .AllowAnyOrigin()
                     .AllowCredentials();
                });
            });

            services.AddSingleton<MessagesEndPoint>();
            services.AddSingleton<LegacyEndPoint>();
            services.AddSingleton<LegacyHubEndPoint>();
        }

        // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
        public void Configure(IApplicationBuilder app, IHostingEnvironment env)
        {
            app.UseFileServer();

            if (env.IsDevelopment())
            {
                app.UseDeveloperExceptionPage();
            }

            app.UseCors("Everything");

            app.UseSignalR(routes =>
            {
                routes.MapHub<DynamicChat>("/dynamic");
                routes.MapHub<Chat>("/default");
                routes.MapHub<Streaming>("/streaming");
                routes.MapHub<HubTChat>("/hubT");
            });

            app.UseSockets(routes =>
            {
                routes.MapEndPoint<MessagesEndPoint>("/chat");

                routes.MapLegacySocket("/foo", new HttpSocketOptions(), builder =>
                {
                    builder.UseEndPoint<LegacyEndPoint>();
                });

                routes.MapLegacySocket("/signalr", new HttpSocketOptions(), builder =>
                {
                    builder.UseEndPoint<LegacyHubEndPoint>();
                });
            });
        }

        public class LegacyJsonHubProtocol : IHubProtocol
        {
            public JsonSerializer Serializer { get; } = new JsonSerializer();
            public string Name => "legacyJson";

            public TransferFormat TransferFormat => TransferFormat.Text;

            private string _hubName;

            public LegacyJsonHubProtocol(string hubName)
            {
                _hubName = hubName;
            }

            public bool TryParseMessages(ReadOnlyMemory<byte> input, IInvocationBinder binder, IList<HubMessage> messages)
            {
                // REVIEW: This is a hack, we should implement a TextReader over the
                // pipe reader
                // PS: It's so easy to write bad .NET codez :D

                var obj = JObject.Parse(Encoding.UTF8.GetString(input.ToArray()));

                var hubName = obj.Value<string>("H");
                var invocationId = obj.Value<string>("I");
                var method = obj.Value<string>("M");
                var args = obj.Value<JArray>("A");

                ExceptionDispatchInfo argumentBindingException = null;
                object[] arguments = null;

                try
                {
                    arguments = BindArguments(args, binder.GetParameterTypes(method));
                }
                catch (Exception ex)
                {
                    argumentBindingException = ExceptionDispatchInfo.Capture(ex);
                }

                messages.Add(new InvocationMessage(invocationId, method, argumentBindingException, arguments));
                return true;
            }

            public void WriteMessage(HubMessage message, Stream output)
            {
                if (message is InvocationMessage im)
                {
                    var bytes = GetJsonBytes(new
                    {
                        M = new[] {
                           new {
                            A = im.Arguments,
                            M = im.Target,
                            H = _hubName
                           }
                        }
                    });
                    output.Write(bytes, 0, bytes.Length);
                }
                
                if (message is CompletionMessage cm)
                {
                    var bytes = GetJsonBytes(new
                    {
                        I = cm.InvocationId,
                        R = cm.Result,
                        E = cm.Error
                    });
                    output.Write(bytes, 0, bytes.Length);
                }
            }

            private byte[] GetJsonBytes(object o)
            {
                return Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(o));
            }

            private object[] BindArguments(JArray args, IReadOnlyList<Type> paramTypes)
            {
                var arguments = new object[args.Count];
                if (paramTypes.Count != arguments.Length)
                {
                    throw new InvalidDataException($"Invocation provides {arguments.Length} argument(s) but target expects {paramTypes.Count}.");
                }

                try
                {
                    for (var i = 0; i < paramTypes.Count; i++)
                    {
                        var paramType = paramTypes[i];
                        arguments[i] = args[i].ToObject(paramType, Serializer);
                    }

                    return arguments;
                }
                catch (Exception ex)
                {
                    throw new InvalidDataException("Error binding arguments. Make sure that the types of the provided values match the types of the hub method being invoked.", ex);
                }
            }
        }

        public class LegacyHubEndPoint : EndPoint
        {
            private IServiceProvider _provider;

            private ConcurrentDictionary<string, LegacyJsonHubProtocol> _protocols = new ConcurrentDictionary<string, LegacyJsonHubProtocol>();
            public LegacyHubEndPoint(IServiceProvider provider)
            {
                _provider = provider;
            }

            public override async Task OnConnectedAsync(ConnectionContext connection)
            {
                // Parse the data as a JArray hub names and get the correct hub dispatcher
                // based on the hub name
                var data = connection.Items["connectionData"];
                System.Console.WriteLine(data);

                var hubDispatcherType = typeof(HubDispatcher<>).MakeGenericType(typeof(Chat));

                var hubConnectionContext = new HubConnectionContext(connection, Timeout.InfiniteTimeSpan, _provider.GetRequiredService<ILoggerFactory>());
                hubConnectionContext.Protocol = _protocols.GetOrAdd("chat", key => new LegacyJsonHubProtocol(key));

                var dispatcher = (HubDispatcher)_provider.GetRequiredService(hubDispatcherType);

                await dispatcher.OnConnectedAsync(hubConnectionContext);

                await connection.Transport.Output.WriteAsync(GetJsonBytes(new
                {
                    M = new object[0],
                    S = 1
                }));

                while (true)
                {
                    var result = await connection.Transport.Input.ReadAsync();
                    var buffer = result.Buffer;

                    if (!buffer.IsEmpty)
                    {
                        var messages = new List<HubMessage>();
                        if (hubConnectionContext.Protocol.TryParseMessages(buffer.ToArray(), dispatcher, messages))
                        {
                            foreach (var message in messages)
                            {
                                await dispatcher.DispatchMessageAsync(hubConnectionContext, message);
                            }
                        }

                    }
                    else if (result.IsCompleted)
                    {
                        break;
                    }
                    connection.Transport.Input.AdvanceTo(buffer.End);
                }

                // TODO: Errors
                await dispatcher.OnDisconnectedAsync(hubConnectionContext, null);
            }

            private byte[] GetJsonBytes(object o)
            {
                return Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(o));
            }
        }

        public class LegacyEndPoint : EndPoint
        {
            public override async Task OnConnectedAsync(ConnectionContext connection)
            {
                // Send the init payload
                await connection.Transport.Output.WriteAsync(GetJsonBytes(new
                {
                    M = new object[0],
                    S = 1
                }));

                if (connection.Features.Get<IConnectionInherentKeepAliveFeature>() == null)
                {
                    connection.Features.Get<IConnectionHeartbeatFeature>()?.OnHeartbeat(state =>
                    {
                        // Keep alive
                        // _ = connection.Transport.Output.WriteAsync(Encoding.UTF8.GetBytes("{}"));
                    },
                    null);
                }

                while (true)
                {
                    var result = await connection.Transport.Input.ReadAsync();
                    var buffer = result.Buffer;

                    if (!buffer.IsEmpty)
                    {
                        System.Console.WriteLine($"Received: {buffer.Length} bytes");

                        await connection.Transport.Output.WriteAsync(GetJsonBytes(
                            new
                            {
                                M = new[] { Encoding.UTF8.GetString(buffer.ToArray()) }
                            }
                        ));
                    }
                    else if (result.IsCompleted)
                    {
                        break;
                    }
                    connection.Transport.Input.AdvanceTo(buffer.End);
                }
            }

            private byte[] GetJsonBytes(object o)
            {
                return Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(o));
            }
        }
    }
}
