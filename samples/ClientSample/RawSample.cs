// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Buffers;
using System.Diagnostics;
using System.IO;
using System.IO.Pipelines;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Protocols;
using Microsoft.AspNetCore.Sockets;
using Microsoft.AspNetCore.Sockets.Client;
using Microsoft.Extensions.CommandLineUtils;
using Microsoft.Extensions.Logging;

namespace ClientSample
{
    internal class RawSample
    {
        internal static void Register(CommandLineApplication app)
        {
            app.Command("raw", cmd =>
            {
                cmd.Description = "Tests a connection to an endpoint";

                var baseUrlArgument = cmd.Argument("<BASEURL>", "The URL to the Chat EndPoint to test");

                cmd.OnExecute(() => ExecuteAsync(baseUrlArgument.Value));
            });
        }

        public static async Task<int> ExecuteAsync(string baseUrl)
        {
            baseUrl = string.IsNullOrEmpty(baseUrl) ? "http://localhost:5000/chat" : baseUrl;

            var loggerFactory = new LoggerFactory();
            var logger = loggerFactory.CreateLogger<Program>();

            Console.WriteLine($"Connecting to {baseUrl}...");
            var connection = new HttpConnection(new Uri(baseUrl), loggerFactory);
            try
            {
                await connection.StartAsync(TransferFormat.Text);

                Console.WriteLine($"Connected to {baseUrl}");
                var shutdown = new TaskCompletionSource<object>();
                Console.CancelKeyPress += (sender, a) =>
                {
                    a.Cancel = true;
                    shutdown.TrySetResult(null);
                };

                _ = Task.Run(() => SendLoop(Console.In, connection.Transport.Output));
                _ = Task.Run(() => ReceiveLoop(Console.Out, connection.Transport.Input));

                await shutdown.Task;
            }
            catch (AggregateException aex) when (aex.InnerExceptions.All(e => e is OperationCanceledException))
            {
            }
            catch (OperationCanceledException)
            {
            }
            finally
            {
                await connection.DisposeAsync();
            }
            return 0;
        }

        private static async Task ReceiveLoop(TextWriter output, PipeReader input)
        {
            while (true)
            {
                var result = await input.ReadAsync();
                var buffer = result.Buffer;

                try
                {
                    if (result.IsCanceled || (buffer.IsEmpty && result.IsCompleted))
                    {
                        return;
                    }

                    await output.WriteLineAsync(Encoding.UTF8.GetString(buffer.ToArray()));
                }
                finally
                {
                    input.AdvanceTo(buffer.End);
                }
            }
        }

        private static async Task SendLoop(TextReader input, PipeWriter output)
        {
            while (true)
            {
                var result = await input.ReadLineAsync();
                await output.WriteAsync(Encoding.UTF8.GetBytes(result));
            }
        }
    }
}
