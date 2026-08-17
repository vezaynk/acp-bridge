namespace AcpKit.Bridge;

/// <summary>
/// A stdio-to-WebSocket pipe for the Agent Client Protocol.
/// </summary>
/// <remarks>
/// ACP agents speak newline-delimited JSON-RPC over stdin and stdout, which means a client can
/// only talk to an agent it is able to spawn as a local process. This moves that boundary:
/// <c>listen</c> runs where the agent lives, <c>connect</c> runs where the client lives and
/// pretends to be the agent, and the conversation between them is unchanged.
/// </remarks>
public static class Program
{
    private const string Usage = """
        acp-bridge listen [--bind HOST:PORT] -- <agent command>
        acp-bridge connect <ws-url>

        listen  hosts /acp, spawning the agent once per connection, and prints a single
                'listening <url>' line on stdout. Bind to port 0 to be assigned one.
        connect uses stdin and stdout as the ACP channel, so a client can spawn it in
                place of the agent.
        """;

    public static async Task<int> Main(string[] args)
    {
        if (args.Length == 0 || args[0] is "-h" or "--help" or "help")
        {
            Console.Error.WriteLine(Usage);
            return args.Length == 0 ? 2 : 0;
        }

        using var shutdown = new CancellationTokenSource();
        Console.CancelKeyPress += (_, e) =>
        {
            // Unwind rather than die, so the agent gets closed stdin and the socket gets a
            // close frame instead of both being severed mid-message.
            e.Cancel = true;
            shutdown.Cancel();
        };

        try
        {
            return args[0] switch
            {
                "listen" => await ListenCommand.RunAsync(args[1..], shutdown.Token),
                "connect" => await ConnectCommand.RunAsync(args[1..], shutdown.Token),
                _ => Fail($"unknown command '{args[0]}' (expected listen or connect)"),
            };
        }
        catch (OperationCanceledException)
        {
            return 0;
        }
        catch (Exception e)
        {
            Console.Error.WriteLine("acp-bridge: " + e.Message);
            return 1;
        }
    }

    internal static int Fail(string message)
    {
        Console.Error.WriteLine("acp-bridge: " + message);
        return 2;
    }
}
