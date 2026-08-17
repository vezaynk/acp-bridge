# acp-bridge

Run an [Agent Client Protocol](https://agentclientprotocol.com/) agent over a WebSocket
instead of a local pipe.

ACP agents speak newline-delimited JSON-RPC over stdin and stdout, which means a client can
normally only talk to an agent it is able to spawn as a local process. This moves that
boundary: `listen` runs where the agent lives, `connect` runs where the client lives and
stands in for the agent, and the conversation between them is untouched.

```sh
dotnet tool install -g AcpKit.Bridge
```

## Use

On the machine with the agent:

```sh
acp-bridge listen --bind 0.0.0.0:7100 -- your-agent acp
# listening ws://0.0.0.0:7100/acp
```

On the machine with the client, point it at `acp-bridge` as though it were the agent:

```sh
acp-bridge connect ws://the-other-host:7100/acp
```

A client that spawns agents from configuration needs no code change — only the command it
spawns:

```json
{ "spawn": ["acp-bridge", "connect", "ws://the-other-host:7100/acp"] }
```

`listen` prints exactly one `listening <url>` line on stdout and is silent there afterwards,
so a supervisor can bind to port `0` and read back the port it was given. Everything else,
including the agent's own stderr, goes to stderr.

One connection at a time. A second while the first is live gets `409`, because two clients
sharing an agent would interleave their JSON-RPC ids into one stream and the answers would go
to whoever asked last.

## It does not understand ACP

The bridge forwards bytes. It never parses a message, so there is no protocol version, message
shape, or future extension it can be wrong about — an agent speaking something this was never
built for still works, because "this" was never built for anything in particular.

What it does handle is framing: one JSON-RPC object per line becomes one text frame and back,
CRLF is accepted, and a message is never truncated. That last part matters more than it
sounds — a single `session/update` carrying a diff or a base64 terminal snapshot runs to
megabytes and fits in no single read, pipe segment, or frame.

## What it is not

Not a transport for the open internet. There is no authentication and no encryption: `ws://`
carries the whole conversation in the clear, and anyone who can reach the port can drive the
agent. Put it on a private network, an SSH tunnel, or behind a reverse proxy terminating TLS.

Not a supervisor either. If the listener disappears, the agent it spawned goes with it, but a
client that has stopped talking is not something the bridge will notice on the client's
behalf — cancellation and process exit remain the client's business.

## Building

```sh
dotnet build acp-bridge.slnx
ACP_BRIDGE=$PWD/artifacts/bin/AcpKit.Bridge/debug/acp-bridge \
  dotnet run --project tests/AcpKit.Bridge.Tests
```

The tests drive both halves as real processes and check what arrives: a round trip, a 2 MiB
payload, two hundred messages in order, a CRLF-terminated agent, a refused second connection,
and — the one that has actually caught a bug — that the client's process exits when the
far-side agent dies.

## Dependencies

None at runtime. Kestrel hosts the listener and `System.IO.Pipelines` does the framing, both
from the ASP.NET Core shared framework, so nothing has to be resolved from NuGet.

Despite the package name, it does not depend on [AcpKit](https://github.com/vezaynk/acpkit).
A bridge that forwards bytes has no use for protocol types, and giving it some would only give
it ways to be wrong.

## License

Apache-2.0. See [LICENSE](LICENSE).
