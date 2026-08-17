using System.Diagnostics;
using System.Net.WebSockets;

namespace AcpKit.Bridge;

/// <summary>One WebSocket paired with one spawned ACP agent, for the life of that socket.</summary>
internal static class AgentSession
{
    /// <summary>Spawn the agent and pump until the socket or the process ends.</summary>
    public static async Task RunAsync(
        WebSocket socket, IReadOnlyList<string> argv, CancellationToken cancellationToken)
    {
        var start = new ProcessStartInfo(argv[0])
        {
            UseShellExecute = false,
            RedirectStandardInput = true,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
        };

        for (var i = 1; i < argv.Count; i++)
        {
            start.ArgumentList.Add(argv[i]);
        }

        using var agent = Process.Start(start)
            ?? throw new InvalidOperationException($"Could not start '{argv[0]}'.");

        // Agents write banners, warnings, and their own diagnostics to stderr. Relaying it
        // keeps a misconfigured agent debuggable from the side that launched the bridge; it is
        // never protocol traffic, so it must not go anywhere near the socket.
        var stderr = RelayAsync(agent.StandardError, cancellationToken);

        try
        {
            await FramePump.RunAsync(
                socket,
                agent.StandardOutput.BaseStream,
                agent.StandardInput.BaseStream,
                cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            // Closing stdin is the polite exit: an agent reading NDJSON sees end of input and
            // shuts down on its own. The kill is the backstop for one that does not.
            try
            {
                agent.StandardInput.Close();
            }
            catch (IOException)
            {
                // Already gone.
            }

            if (!agent.HasExited)
            {
                try
                {
                    agent.Kill(entireProcessTree: true);
                }
                catch (InvalidOperationException)
                {
                    // Exited between the check and the kill.
                }
            }

            try
            {
                await agent.WaitForExitAsync(CancellationToken.None).ConfigureAwait(false);
                await stderr.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // Shutting down.
            }
        }
    }

    private static async Task RelayAsync(StreamReader reader, CancellationToken cancellationToken)
    {
        try
        {
            while (await reader.ReadLineAsync(cancellationToken).ConfigureAwait(false) is { } line)
            {
                await Console.Error.WriteLineAsync(line).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
        {
            // The listener is going down.
        }
    }
}
