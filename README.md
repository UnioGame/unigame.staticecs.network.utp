# Static ECS Network Unity Transport

Unity Transport adapter for the complete-packet `INetworkTransport` contract.

## Capabilities

- Exposes reliable and unreliable complete-packet capabilities, including `PacketHeader`.
- Copies each received native packet directly once into a pooled lease.
- Uses bounded receive queues and a fixed-capacity reliable FIFO of existing leases under UTP backpressure.
- Retries reliable packets in FIFO order only after a later driver update processes ACKs.
- Reports channel traffic, failures, queue depth/high-water, overflow, disconnect, and lease diagnostics.

## Usage

```csharp
var settings = UnityTransportSettings.Default;
using var client = new UnityTransportClientHost(settings);
client.Update();
client.Flush();
```

Call `Update` before protocol receive and `Flush` after protocol send. Server endpoints
from `UnityTransportServerHost.TryAccept` are passed to the transport-neutral server;
drain disconnect notifications after each update.

## Configuration

- Default port: `7777`.
- Maximum unreliable complete packet: `1400` bytes.
- Maximum reliable complete packet: `64 KiB`.
- Snapshot body capacity is `MaxReliablePayloadBytes - 113` bytes.
- Queue capacities are bounded; snapshot chunking derives from protocol and transport limits and has no config knob.
- A pending snapshot tick blocks enqueue of a different snapshot tick until the prior reliable FIFO drains.
- See [runtime config schema v2](../../../docs/guides/network-client-server-runtime-config.md) for separated roles.
