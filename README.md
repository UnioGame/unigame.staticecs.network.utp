# Static ECS Network Unity Transport

## Capabilities

- Adapts Unity Transport 2.6 to the exact-packet `INetworkTransport` contract.
- Keeps client/server driver ownership outside the transport-neutral protocol package.
- Maps reliable packets to fragmentation plus reliable-sequenced delivery and commands to unreliable-sequenced delivery.
- Copies received native data into bounded `NetworkBufferPool` leases.

## Usage

Create one `UnityTransportClientHost` or `UnityTransportServerHost`, call `Update` before the protocol receive systems and `Flush` after protocol send systems, and dispose the host at shutdown. Server endpoints returned by `TryAccept` are passed to `NetworkServer.AddConnection`; after each update, drain `TryDequeueDisconnected` and remove the matching server connections.

## Configuration

`UnityTransportSettings.Default` uses port 7777, a 1400-byte unreliable packet limit, a 64 KiB reliable limit, and bounded receive queues. Remote disconnect notifications are FIFO and buffered up to `MaximumConnections`; if the caller does not drain them, the newest overflow is dropped and counted in `DroppedPackets`. Application-level chunking above 64 KiB is intentionally not provided.

`UnityTransportDiagnostics` keeps cumulative reliable/unreliable packet and byte counters, receive queue overflows, malformed packet rejections, send failures, drops, disconnects, queued packets, and outstanding receive leases. Queue, malformed, and send-failure counters are additive diagnostics; `DroppedPackets` remains the aggregate rejection/drop counter.
