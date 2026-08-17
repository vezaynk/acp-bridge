using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace AcpKit.Bridge;

/// <summary>
/// The far side: hosts <c>/acp</c> as a WebSocket and, per connection, spawns the agent and
/// pumps frames until either end goes away.
/// </summary>
internal static class ListenCommand
{
    public static async Task<int> RunAsync(string[] args, CancellationToken cancellationToken)
    {
        var bind = "127.0.0.1:0";
        string[]? agent = null;

        for (var i = 0; i < args.Length; i++)
        {
            var argument = args[i];

            if (argument == "--")
            {
                agent = args[(i + 1)..];
                break;
            }

            if (argument == "--bind")
            {
                if (i + 1 >= args.Length)
                {
                    return Program.Fail("--bind needs HOST:PORT");
                }

                bind = args[++i];
                continue;
            }

            if (argument.StartsWith('-'))
            {
                return Program.Fail($"unknown listen flag '{argument}'");
            }

            agent = args[i..];
            break;
        }

        if (agent is not { Length: > 0 })
        {
            return Program.Fail("listen needs an agent command, after -- if it takes flags of its own");
        }

        var builder = WebApplication.CreateSlimBuilder();
        builder.Logging.ClearProviders();
        builder.WebHost.SuppressStatusMessages(true);
        builder.WebHost.UseUrls("http://" + bind);

        var app = builder.Build();
        app.UseWebSockets();

        // One agent at a time. Two clients sharing a process would interleave their JSON-RPC
        // ids into one stream, and the answers would go to whoever asked last.
        var busy = new SemaphoreSlim(1, 1);

        app.Map("/acp", async (HttpContext context) =>
        {
            if (!context.WebSockets.IsWebSocketRequest)
            {
                context.Response.StatusCode = StatusCodes.Status400BadRequest;
                return;
            }

            if (!await busy.WaitAsync(0).ConfigureAwait(false))
            {
                context.Response.StatusCode = StatusCodes.Status409Conflict;
                return;
            }

            try
            {
                using var socket = await context.WebSockets.AcceptWebSocketAsync().ConfigureAwait(false);
                await AgentSession.RunAsync(socket, agent, context.RequestAborted).ConfigureAwait(false);
            }
            finally
            {
                busy.Release();
            }
        });

        await app.StartAsync(cancellationToken).ConfigureAwait(false);

        var bound = app.Urls.FirstOrDefault()
            ?? throw new InvalidOperationException("The server bound no address.");
        var url = bound.Replace("http://", "ws://", StringComparison.Ordinal) + "/acp";

        // One machine-readable line, then stdout is silent. Callers bind to port 0 and read
        // this to learn where the server actually landed.
        await Console.Out.WriteLineAsync("listening " + url).ConfigureAwait(false);
        await Console.Out.FlushAsync(cancellationToken).ConfigureAwait(false);

        await app.WaitForShutdownAsync(cancellationToken).ConfigureAwait(false);
        return 0;
    }
}
