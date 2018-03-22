// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Buffers;
using System.Text;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Protocols;
using Microsoft.AspNetCore.Protocols.Features;
using Microsoft.AspNetCore.Sockets;
using Microsoft.Extensions.DependencyInjection;
using MsgPack.Serialization;
using Newtonsoft.Json;
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
            });
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
