using System.Buffers;
using System.IO.Pipelines;
using System.Net.WebSockets;

namespace AcpKit.Bridge;

/// <summary>
/// Moves ACP traffic between a newline-delimited byte stream and a WebSocket: one JSON-RPC
/// object per line becomes one text frame, and back.
/// </summary>
/// <remarks>
/// <para>
/// Nothing here parses ACP, and that is the design rather than an omission. A bridge that
/// decoded messages would have to understand every construct the protocol has or will have,
/// and would break the day an agent sends one it does not. Forwarding bytes verbatim cannot
/// corrupt a payload and cannot go out of date.
/// </para>
/// <para>
/// Receive and send run concurrently, which is the supported <see cref="WebSocket"/> pattern:
/// one send and one receive may be outstanding at a time, but not two of either.
/// </para>
/// </remarks>
internal static class FramePump
{
    /// <summary>How long the surviving direction gets to notice the other one ended.</summary>
    private static readonly TimeSpan UnwindGrace = TimeSpan.FromSeconds(2);

    /// <summary>Pump until either direction ends, then unwind the other.</summary>
    public static async Task RunAsync(
        WebSocket socket, Stream incoming, Stream outgoing, CancellationToken cancellationToken)
    {
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var token = linked.Token;

        var toSocket = CopyLinesToSocketAsync(incoming, socket, token);
        var fromSocket = CopyFramesToStreamAsync(socket, outgoing, token);

        var first = await Task.WhenAny(toSocket, fromSocket).ConfigureAwait(false);
        await linked.CancelAsync().ConfigureAwait(false);

        // Give the other direction a chance to unwind, but do not wait on it indefinitely.
        // A read already pending on stdin is not reliably cancellable — the token is observed
        // between reads, not during one — so waiting for both to finish can hang forever on a
        // read that will never complete because nothing more is ever going to be typed. When
        // the far side dies, this process has to exit; a client watching for its agent to
        // disappear is relying on exactly that.
        var unwound = Task.WhenAll(toSocket, fromSocket);
        await Task.WhenAny(unwound, Task.Delay(UnwindGrace, CancellationToken.None)).ConfigureAwait(false);

        if (unwound.IsCompleted)
        {
            try
            {
                await unwound.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // Expected: cancelling is how the surviving direction is told the other ended.
            }
        }

        // Surface whichever side finished first, so a genuine failure is not swallowed by the
        // cancellation it triggered.
        await first.ConfigureAwait(false);
    }

    /// <summary>
    /// Split the stream on newlines and send each line as one text frame.
    /// </summary>
    /// <remarks>
    /// A pipe rather than a <c>StreamReader</c>: the payload is already UTF-8 and the socket
    /// wants UTF-8, so decoding to a string only to re-encode would be waste on every message.
    /// There is no line-length ceiling either — a <c>session/update</c> carrying a diff or a
    /// base64 terminal snapshot runs to megabytes.
    /// </remarks>
    private static async Task CopyLinesToSocketAsync(
        Stream incoming, WebSocket socket, CancellationToken cancellationToken)
    {
        var reader = PipeReader.Create(incoming);

        try
        {
            while (true)
            {
                var result = await reader.ReadAsync(cancellationToken).ConfigureAwait(false);
                var buffer = result.Buffer;

                while (buffer.PositionOf((byte)'\n') is { } newline)
                {
                    var line = Trim(buffer.Slice(0, newline));
                    if (line.Length > 0)
                    {
                        if (socket.State != WebSocketState.Open)
                        {
                            return;
                        }

                        await SendAsync(socket, line, cancellationToken).ConfigureAwait(false);
                    }

                    buffer = buffer.Slice(buffer.GetPosition(1, newline));
                }

                if (result.IsCompleted)
                {
                    // A trailing line with no terminator is what a process that exits
                    // mid-write leaves behind, and it is still a message.
                    var tail = Trim(buffer);
                    if (tail.Length > 0 && socket.State == WebSocketState.Open)
                    {
                        await SendAsync(socket, tail, cancellationToken).ConfigureAwait(false);
                    }

                    reader.AdvanceTo(buffer.End);
                    return;
                }

                reader.AdvanceTo(buffer.Start, buffer.End);
            }
        }
        finally
        {
            await reader.CompleteAsync().ConfigureAwait(false);
        }
    }

    /// <summary>Send one frame, joining pipe segments only when the payload spans them.</summary>
    private static async ValueTask SendAsync(
        WebSocket socket, ReadOnlySequence<byte> payload, CancellationToken cancellationToken)
    {
        if (payload.IsSingleSegment)
        {
            await socket.SendAsync(payload.First, WebSocketMessageType.Text, endOfMessage: true, cancellationToken)
                .ConfigureAwait(false);
            return;
        }

        // Split across segments. Send each piece with endOfMessage false so the far side still
        // sees exactly one message, and nothing has to be copied to make it contiguous.
        var remaining = payload.Length;
        foreach (var segment in payload)
        {
            remaining -= segment.Length;
            await socket.SendAsync(segment, WebSocketMessageType.Text, endOfMessage: remaining == 0, cancellationToken)
                .ConfigureAwait(false);
        }
    }

    /// <summary>Write each received frame to the stream as one newline-terminated line.</summary>
    private static async Task CopyFramesToStreamAsync(
        WebSocket socket, Stream outgoing, CancellationToken cancellationToken)
    {
        var scratch = new byte[16 * 1024];
        var message = new ArrayBufferWriter<byte>(64 * 1024);

        while (socket.State == WebSocketState.Open && !cancellationToken.IsCancellationRequested)
        {
            message.Clear();
            ValueWebSocketReceiveResult result;

            do
            {
                result = await socket.ReceiveAsync(scratch.AsMemory(), cancellationToken).ConfigureAwait(false);

                if (result.MessageType == WebSocketMessageType.Close)
                {
                    return;
                }

                // Binary frames are not ACP. Skipping the payload but continuing to drain the
                // message keeps a stray frame from desynchronising the stream.
                if (result.MessageType == WebSocketMessageType.Binary)
                {
                    continue;
                }

                message.Write(scratch.AsSpan(0, result.Count));
            }
            while (!result.EndOfMessage);

            if (message.WrittenCount == 0)
            {
                continue;
            }

            await outgoing.WriteAsync(message.WrittenMemory, cancellationToken).ConfigureAwait(false);
            await outgoing.WriteAsync(Newline, cancellationToken).ConfigureAwait(false);
            await outgoing.FlushAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    private static ReadOnlyMemory<byte> Newline { get; } = new[] { (byte)'\n' };

    /// <summary>Drop a trailing CR, so a CRLF-terminated stream behaves identically.</summary>
    private static ReadOnlySequence<byte> Trim(ReadOnlySequence<byte> line)
    {
        if (line.IsEmpty)
        {
            return line;
        }

        var end = line.GetPosition(line.Length - 1);
        var last = line.Slice(end);
        return last.FirstSpan.Length > 0 && last.FirstSpan[0] == (byte)'\r' ? line.Slice(0, line.Length - 1) : line;
    }
}
