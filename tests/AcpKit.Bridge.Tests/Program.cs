using System.Diagnostics;
using System.Net.WebSockets;
using System.Text.Json;

// Drives a conversation through both halves of the bridge and checks it arrives intact.
//
// Deliberately end to end rather than unit tests of the pump: what can actually be wrong is
// the seam — a frame split across reads, a message larger than a buffer, a CRLF stream, an
// agent that exits mid-turn — and none of that shows up when the pump is handed a
// well-behaved MemoryStream.
//
// This binary is also the agent under test. Invoked with a script name it behaves like an ACP
// agent; invoked with nothing it runs the scenarios. That avoids depending on a real agent
// being installed, and lets a scenario script behaviour a real one would not reliably produce.

if (args.Length > 0 && ScriptedAgent.Handles(args[0]))
{
    return await ScriptedAgent.RunAsync(args[0]);
}

var bridge = Environment.GetEnvironmentVariable("ACP_BRIDGE")
    ?? throw new InvalidOperationException("Set ACP_BRIDGE to the acp-bridge binary under test.");

Console.WriteLine($"acp-bridge conformance: {bridge}");

var failures = 0;
foreach (var (name, run) in Scenarios.All)
{
    using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(60));
    var began = Stopwatch.GetTimestamp();

    try
    {
        await run(bridge, timeout.Token);
        Console.WriteLine($"  ok    {name}  ({Stopwatch.GetElapsedTime(began).TotalMilliseconds:F0} ms)");
    }
    catch (Exception e)
    {
        failures++;
        Console.WriteLine($"  FAIL  {name}");
        Console.WriteLine($"          {e.GetType().Name}: {e.Message}");
    }
}

Console.WriteLine();
Console.WriteLine(failures == 0
    ? $"  {Scenarios.All.Count}/{Scenarios.All.Count} passed"
    : $"  {Scenarios.All.Count - failures}/{Scenarios.All.Count} passed, {failures} failed");

return failures == 0 ? 0 : 1;

/// <summary>An ACP-shaped agent whose behaviour a scenario chooses.</summary>
internal static class ScriptedAgent
{
    public static bool Handles(string script) =>
        script is "echo" or "echo-crlf" or "exit-immediately";

    public static async Task<int> RunAsync(string script)
    {
        if (script == "exit-immediately")
        {
            return 0;
        }

        var terminator = script == "echo-crlf" ? "\r\n" : "\n";
        var stdin = Console.OpenStandardInput();
        var stdout = Console.OpenStandardOutput();

        using var reader = new StreamReader(stdin);
        await using var writer = new StreamWriter(stdout) { AutoFlush = true, NewLine = terminator };

        while (await reader.ReadLineAsync() is { } line)
        {
            if (line.Length > 0)
            {
                await writer.WriteLineAsync(line);
            }
        }

        return 0;
    }
}

internal static class Scenarios
{
    public static IReadOnlyList<(string Name, Func<string, CancellationToken, Task> Run)> All =>
    [
        ("a request and its response survive the round trip", RoundTrip),
        ("a message larger than any buffer arrives whole", LargePayload),
        ("many messages keep their order", Ordering),
        ("a CRLF-terminated agent is understood", CarriageReturns),
        ("the agent's exit ends the client's process", AgentExitPropagates),
        ("a second connection is refused while one is live", SecondConnectionRefused),
    ];

    private static async Task<Link> OpenAsync(string bridge, string script, CancellationToken ct)
    {
        var self = Environment.ProcessPath
            ?? throw new InvalidOperationException("Cannot locate this test binary to use as the agent.");

        var listener = Start(bridge, ["listen", "--bind", "127.0.0.1:0", "--", self, script]);

        // listen prints exactly one line naming where it landed, then stdout goes quiet.
        var announced = await listener.StandardOutput.ReadLineAsync(ct)
            ?? throw new InvalidOperationException("listen exited before announcing a URL");

        if (!announced.StartsWith("listening ", StringComparison.Ordinal))
        {
            throw new InvalidOperationException($"expected 'listening <url>', got: {announced}");
        }

        var url = announced["listening ".Length..];
        var client = Start(bridge, ["connect", url]);
        return new Link(listener, client, url);
    }

    private static Process Start(string path, string[] args)
    {
        var start = new ProcessStartInfo(path)
        {
            RedirectStandardInput = true,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
        };

        foreach (var argument in args)
        {
            start.ArgumentList.Add(argument);
        }

        return Process.Start(start) ?? throw new InvalidOperationException($"could not start {path}");
    }

    private static async Task RoundTrip(string bridge, CancellationToken ct)
    {
        using var link = await OpenAsync(bridge, "echo", ct);

        await link.SendAsync("""{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":1}}""", ct);
        var reply = await link.ReceiveAsync(ct);

        using var document = JsonDocument.Parse(reply);
        Expect(document.RootElement.GetProperty("id").GetInt32() == 1, $"the id survived; got {reply}");
    }

    /// <summary>
    /// A single ACP message routinely runs to megabytes — a diff, or a base64 terminal
    /// snapshot. It fits in no single read, pipe segment, or WebSocket frame.
    /// </summary>
    private static async Task LargePayload(string bridge, CancellationToken ct)
    {
        using var link = await OpenAsync(bridge, "echo", ct);

        var payload = new string('x', 2 * 1024 * 1024);
        var message = string.Concat(
            "{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"big\",\"params\":{\"blob\":\"",
            payload,
            "\"}}");
        await link.SendAsync(message, ct);

        var reply = await link.ReceiveAsync(ct);
        using var document = JsonDocument.Parse(reply);
        var received = document.RootElement.GetProperty("params").GetProperty("blob").GetString();
        Expect(received?.Length == payload.Length, $"2 MiB arrived whole; got {received?.Length ?? -1} chars");
    }

    private static async Task Ordering(string bridge, CancellationToken ct)
    {
        using var link = await OpenAsync(bridge, "echo", ct);

        const int count = 200;
        for (var i = 0; i < count; i++)
        {
            await link.SendAsync($$"""{"jsonrpc":"2.0","id":{{i}},"method":"n"}""", ct);
        }

        for (var i = 0; i < count; i++)
        {
            using var document = JsonDocument.Parse(await link.ReceiveAsync(ct));
            var id = document.RootElement.GetProperty("id").GetInt32();
            Expect(id == i, $"message {i} arrived in order; got {id}");
        }
    }

    private static async Task CarriageReturns(string bridge, CancellationToken ct)
    {
        using var link = await OpenAsync(bridge, "echo-crlf", ct);

        await link.SendAsync("""{"jsonrpc":"2.0","id":7,"method":"crlf"}""", ct);
        var reply = await link.ReceiveAsync(ct);

        Expect(!reply.EndsWith('\r'), "the trailing CR was stripped rather than forwarded");
        using var document = JsonDocument.Parse(reply);
        Expect(document.RootElement.GetProperty("id").GetInt32() == 7, "the message still parsed");
    }

    /// <summary>
    /// A client watching for its agent to die needs that death to reach it. If connect kept
    /// running after the far-side agent exited, the client would wait forever on a turn nobody
    /// is working on.
    /// </summary>
    private static async Task AgentExitPropagates(string bridge, CancellationToken ct)
    {
        using var link = await OpenAsync(bridge, "exit-immediately", ct);
        Expect(await link.WaitForClientExitAsync(TimeSpan.FromSeconds(20), ct),
            "connect exited once the agent was gone");
    }

    private static async Task SecondConnectionRefused(string bridge, CancellationToken ct)
    {
        using var link = await OpenAsync(bridge, "echo", ct);

        // Confirm the first connection is genuinely established before racing a second.
        await link.SendAsync("""{"jsonrpc":"2.0","id":1,"method":"ping"}""", ct);
        await link.ReceiveAsync(ct);

        using var intruder = new ClientWebSocket();
        var refused = false;
        try
        {
            await intruder.ConnectAsync(new Uri(link.Url), ct);
        }
        catch (WebSocketException e)
        {
            refused = e.Message.Contains("409", StringComparison.Ordinal)
                || e.Message.Contains("Conflict", StringComparison.OrdinalIgnoreCase);
        }

        Expect(refused, "a second connection was refused while the first was live");
    }

    private static void Expect(bool condition, string what)
    {
        if (!condition)
        {
            throw new InvalidOperationException(what);
        }
    }
}

/// <summary>Both bridge processes, plus the stdio channel a client would use.</summary>
internal sealed class Link(Process listener, Process client, string url) : IDisposable
{
    public string Url { get; } = url;

    public async Task SendAsync(string line, CancellationToken ct)
    {
        await client.StandardInput.WriteLineAsync(line.AsMemory(), ct);
        await client.StandardInput.FlushAsync(ct);
    }

    public async Task<string> ReceiveAsync(CancellationToken ct) =>
        await client.StandardOutput.ReadLineAsync(ct)
        ?? throw new InvalidOperationException("connect closed its output before replying");

    public async Task<bool> WaitForClientExitAsync(TimeSpan within, CancellationToken ct)
    {
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(ct);
        deadline.CancelAfter(within);

        try
        {
            await client.WaitForExitAsync(deadline.Token);
            return true;
        }
        catch (OperationCanceledException)
        {
            return false;
        }
    }

    public void Dispose()
    {
        foreach (var process in new[] { client, listener })
        {
            try
            {
                if (!process.HasExited)
                {
                    process.Kill(entireProcessTree: true);
                }
            }
            catch (InvalidOperationException)
            {
                // Already gone.
            }

            process.Dispose();
        }
    }
}
