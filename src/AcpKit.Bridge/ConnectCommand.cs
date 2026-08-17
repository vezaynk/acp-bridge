using System.Net.WebSockets;

namespace AcpKit.Bridge;

/// <summary>
/// The near side: stdin and stdout carry the ACP conversation, the WebSocket carries it to
/// wherever the agent actually runs.
/// </summary>
/// <remarks>
/// Written to be launched as if it were the agent itself, so a client that only knows how to
/// spawn a local process gets a remote one without knowing the difference. It exits when
/// either the pipe or the socket ends, because a client watching for the agent to die needs
/// that death to happen.
/// </remarks>
internal static class ConnectCommand
{
    public static async Task<int> RunAsync(string[] args, CancellationToken cancellationToken)
    {
        if (args.Length != 1 || args[0].Length == 0)
        {
            return Program.Fail("connect needs exactly one ws:// or wss:// URL");
        }

        if (!Uri.TryCreate(args[0], UriKind.Absolute, out var url)
            || url.Scheme is not ("ws" or "wss"))
        {
            return Program.Fail($"'{args[0]}' is not a ws:// or wss:// URL");
        }

        using var socket = new ClientWebSocket();
        await socket.ConnectAsync(url, cancellationToken).ConfigureAwait(false);

        try
        {
            await FramePump.RunAsync(
                socket,
                Console.OpenStandardInput(),
                Console.OpenStandardOutput(),
                cancellationToken).ConfigureAwait(false);
            return 0;
        }
        catch (OperationCanceledException)
        {
            return 0;
        }
        finally
        {
            if (socket.State is WebSocketState.Open or WebSocketState.CloseReceived)
            {
                try
                {
                    await socket.CloseAsync(
                        WebSocketCloseStatus.NormalClosure, "end of input", CancellationToken.None)
                        .ConfigureAwait(false);
                }
                catch (WebSocketException)
                {
                    // The far side already went away; nothing to close politely.
                }
            }
        }
    }
}
