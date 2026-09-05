namespace UniGame.StaticEcs.Network.UnityTransport.Tests
{
    using System;
    using System.Net;
    using System.Net.Sockets;
    using System.Text.RegularExpressions;
    using System.Threading;
    using NUnit.Framework;
    using Unity.Networking.Transport;
    using UnityEngine.TestTools;

    internal sealed class UnityTransportLoopbackTests
    {
        /// <summary>Verifies reliable fragmentation and unreliable sequencing in both directions.</summary>
        [Test]
        public void LoopbackTransfersBothChannelsAndReturnsReceiveLeases()
        {
            var settings = Settings(ReservePort());
            using var server = new UnityTransportServerHost(settings);
            using var client = new UnityTransportClientHost(settings);
            var accepted = WaitForConnection(server, client);
            using var sendPool = new NetworkBufferPool(256 * 1024);

            var reliable = Packet(sendPool, PacketFlags.ReliableOrdered,
                UnityTransportSettings.MaximumReliableBytes);
            Assert.That(client.Endpoint.TrySend(reliable), Is.True);
            Assert.That(reliable.Length, Is.Zero);
            client.Flush();
            var reliableSent = client.CaptureDiagnostics();
            Assert.That(reliableSent.ReliableSentPackets, Is.EqualTo(1));
            Assert.That(reliableSent.ReliableSentBytes,
                Is.EqualTo(UnityTransportSettings.MaximumReliableBytes));
            var receivedReliable = WaitForPacket(server, client, accepted);
            Assert.That(receivedReliable.Length,
                Is.EqualTo(UnityTransportSettings.MaximumReliableBytes));
            var reliableDiagnostics = server.CaptureDiagnostics();
            Assert.That(reliableDiagnostics.OutstandingLeases, Is.EqualTo(1));
            Assert.That(reliableDiagnostics.ReliableReceivedPackets, Is.EqualTo(1));
            Assert.That(reliableDiagnostics.ReliableReceivedBytes,
                Is.EqualTo(UnityTransportSettings.MaximumReliableBytes));
            receivedReliable.Dispose();
            Assert.That(server.CaptureDiagnostics().OutstandingLeases, Is.Zero);

            var unreliable = Packet(sendPool, PacketFlags.UnreliableSequenced,
                settings.MaximumUnreliableBytes);
            Assert.That(accepted.TrySend(unreliable), Is.True);
            Assert.That(unreliable.Length, Is.Zero);
            server.Flush();
            var unreliableSent = server.CaptureDiagnostics();
            Assert.That(unreliableSent.UnreliableSentPackets, Is.EqualTo(1));
            Assert.That(unreliableSent.UnreliableSentBytes,
                Is.EqualTo(settings.MaximumUnreliableBytes));
            var receivedUnreliable = WaitForPacket(server, client, client.Endpoint);
            Assert.That(receivedUnreliable.Length,
                Is.EqualTo(settings.MaximumUnreliableBytes));
            var unreliableDiagnostics = client.CaptureDiagnostics();
            Assert.That(unreliableDiagnostics.OutstandingLeases, Is.EqualTo(1));
            Assert.That(unreliableDiagnostics.UnreliableReceivedPackets, Is.EqualTo(1));
            Assert.That(unreliableDiagnostics.UnreliableReceivedBytes,
                Is.EqualTo(settings.MaximumUnreliableBytes));
            receivedUnreliable.Dispose();
            Assert.That(client.CaptureDiagnostics().OutstandingLeases, Is.Zero);
        }

        /// <summary>Verifies a multi-chunk snapshot at the exact reliable packet boundary.</summary>
        [Test]
        public void SnapshotChunksAtReliableBoundaryCrossLoopback()
        {
            var settings = Settings(ReservePort());
            using var server = new UnityTransportServerHost(settings);
            using var client = new UnityTransportClientHost(settings);
            var accepted = WaitForConnection(server, client);
            using var pool = new NetworkBufferPool(256 * 1024);
            var bodyBytes = UnityTransportSettings.MaximumReliableBytes -
                            PacketHeader.Size - SnapshotChunkHeader.Size;
            var beforeSupersede = default(UnityTransportDiagnostics);

            for (uint index = 0; index < 3; index++)
            {
                var snapshotTick = index < 2 ? 1u : 2u;
                var chunkIndex = index < 2 ? index : 0u;
                var payload = new byte[SnapshotChunkHeader.Size + bodyBytes];
                var chunk = new SnapshotChunkHeader
                {
                    PayloadKind = SnapshotPayloadKind.Keyframe,
                    SnapshotTick = snapshotTick,
                    TotalLength = checked((uint)(bodyBytes * 2)),
                    TotalHash = 1,
                    ChunkIndex = chunkIndex,
                    ChunkCount = 2
                };
                Assert.That(chunk.TryWrite(payload), Is.True);
                var header = new PacketHeader
                {
                    Kind = PacketKind.SnapshotChunk,
                    Flags = PacketFlags.ReliableOrdered,
                    PacketSequence = chunkIndex + 1,
                    ServerTick = snapshotTick
                };
                Assert.That(NetworkPacket.TryEncode(pool, header, payload,
                    out var packet), Is.True);
                Assert.That(packet.Length,
                    Is.EqualTo(UnityTransportSettings.MaximumReliableBytes));
                if (index == 2)
                    beforeSupersede = client.CaptureDiagnostics();
                Assert.That(client.Endpoint.TrySend(packet), Is.True);
                if (index == 2)
                {
                    Assert.That(packet.Length, Is.Zero);
                    var afterSupersede = client.CaptureDiagnostics();
                    Assert.That(afterSupersede.DroppedPackets,
                        Is.EqualTo(beforeSupersede.DroppedPackets + 1));
                    Assert.That(afterSupersede.SendFailures,
                        Is.EqualTo(beforeSupersede.SendFailures));
                    Assert.That(afterSupersede.SendFailures, Is.Zero);
                    Assert.That(afterSupersede.ReliableSendQueueOverflows,
                        Is.EqualTo(beforeSupersede.ReliableSendQueueOverflows));
                }
            }

            var pending = client.CaptureDiagnostics();
            Assert.That(pending.PendingReliablePackets, Is.EqualTo(1));
            Assert.That(pending.PendingReliableBytes,
                Is.EqualTo(UnityTransportSettings.MaximumReliableBytes));
            Assert.That(pending.PendingReliablePacketsHighWater, Is.EqualTo(1));
            Assert.That(pending.PendingReliableBytesHighWater,
                Is.EqualTo(UnityTransportSettings.MaximumReliableBytes));
            client.Flush();
            for (uint index = 0; index < 2; index++)
            {
                using var received = WaitForPacket(server, client, accepted);
                Assert.That(received.Length,
                    Is.EqualTo(UnityTransportSettings.MaximumReliableBytes));
                Assert.That(NetworkPacket.TryDecode(received, out var header,
                    out var payload), Is.True);
                Assert.That(header.Kind, Is.EqualTo(PacketKind.SnapshotChunk));
                Assert.That(SnapshotChunkHeader.TryRead(payload.Span,
                    out var chunk), Is.True);
                Assert.That(chunk.ChunkIndex, Is.EqualTo(index));
                Assert.That(chunk.ChunkCount, Is.EqualTo(2));
                if (index == 0)
                    Assert.That(client.CaptureDiagnostics().PendingReliablePackets,
                        Is.EqualTo(1));
            }
            Assert.That(client.CaptureDiagnostics().PendingReliablePackets,
                Is.Zero);
            for (var index = 0; index < 16; index++)
            {
                client.Update();
                server.Update();
            }
            Assert.That(accepted.TryReceive(out _), Is.False);
            Assert.That(server.CaptureDiagnostics().OutstandingLeases, Is.Zero);
            Assert.That(pool.CaptureDiagnostics().OutstandingLeases, Is.Zero);
        }

        /// <summary>Verifies a newer snapshot supersedes an older queued snapshot without send failure.</summary>
        [Test]
        public void NewerSnapshotSupersedesQueuedOlderSnapshot()
        {
            const int reliableWindow = 64;
            var settings = Settings(ReservePort());
            using var server = new UnityTransportServerHost(settings);
            using var client = new UnityTransportClientHost(settings);
            var accepted = WaitForConnection(server, client);
            using var pool = new NetworkBufferPool(256 * 1024);

            for (var index = 0; index < reliableWindow; index++)
                Assert.That(accepted.TrySend(Packet(pool,
                    PacketFlags.ReliableOrdered, PacketHeader.Size)), Is.True);

            var payload = new byte[SnapshotChunkHeader.Size + 1];
            var chunk = new SnapshotChunkHeader
            {
                PayloadKind = SnapshotPayloadKind.Keyframe,
                SnapshotTick = 1,
                TotalLength = 1,
                TotalHash = 1,
                ChunkIndex = 0,
                ChunkCount = 1
            };
            Assert.That(chunk.TryWrite(payload), Is.True);
            var oldHeader = new PacketHeader
            {
                Kind = PacketKind.SnapshotChunk,
                Flags = PacketFlags.ReliableOrdered,
                PacketSequence = 1,
                ServerTick = 1
            };
            Assert.That(NetworkPacket.TryEncode(pool, oldHeader, payload,
                out var oldSnapshot), Is.True);
            Assert.That(accepted.TrySend(oldSnapshot), Is.True);

            var pending = server.CaptureDiagnostics();
            Assert.That(pending.PendingReliablePackets, Is.EqualTo(1));
            Assert.That(pending.PendingReliablePacketsHighWater, Is.EqualTo(1));
            Assert.That(pending.SendFailures, Is.Zero);
            Assert.That(pending.ReliableSendQueueOverflows, Is.Zero);

            chunk.SnapshotTick = 2;
            Assert.That(chunk.TryWrite(payload), Is.True);
            var newerHeader = new PacketHeader
            {
                Kind = PacketKind.SnapshotChunk,
                Flags = PacketFlags.ReliableOrdered,
                PacketSequence = 2,
                ServerTick = 2
            };
            Assert.That(NetworkPacket.TryEncode(pool, newerHeader, payload,
                out var newerSnapshot), Is.True);
            Assert.That(accepted.TrySend(newerSnapshot), Is.True);
            Assert.That(newerSnapshot.Length, Is.Zero);

            var superseded = server.CaptureDiagnostics();
            Assert.That(superseded.DroppedPackets,
                Is.EqualTo(pending.DroppedPackets + 1));
            Assert.That(superseded.SendFailures, Is.Zero);
            Assert.That(superseded.ReliableSendQueueOverflows, Is.Zero);
            Assert.That(superseded.PendingReliablePackets, Is.EqualTo(1));

            chunk.SnapshotTick = 0;
            Assert.That(chunk.TryWrite(payload), Is.True);
            var olderHeader = new PacketHeader
            {
                Kind = PacketKind.SnapshotChunk,
                Flags = PacketFlags.ReliableOrdered,
                PacketSequence = 3,
                ServerTick = 0
            };
            Assert.That(NetworkPacket.TryEncode(pool, olderHeader, payload,
                out var olderSnapshot), Is.True);
            var beforeOlder = server.CaptureDiagnostics();
            Assert.That(accepted.TrySend(olderSnapshot), Is.False);
            Assert.That(olderSnapshot.Length, Is.Zero);
            var afterOlder = server.CaptureDiagnostics();
            Assert.That(afterOlder.DroppedPackets,
                Is.EqualTo(beforeOlder.DroppedPackets + 1));
            Assert.That(afterOlder.SendFailures,
                Is.EqualTo(beforeOlder.SendFailures + 1));
            Assert.That(afterOlder.PendingReliablePackets, Is.EqualTo(1));
            Assert.That(afterOlder.ReliableSendQueueOverflows,
                Is.EqualTo(beforeOlder.ReliableSendQueueOverflows));

            server.Flush();
            var received = 0;
            WaitUntil(() =>
            {
                client.Update();
                while (client.Endpoint.TryReceive(out var packet))
                {
                    packet.Dispose();
                    received++;
                }
                client.Flush();
                server.Update();
                server.Flush();
                return received == reliableWindow + 1 &&
                    server.CaptureDiagnostics().PendingReliablePackets == 0;
            }, "Queued snapshot did not drain after client acknowledgement.");

            var drained = server.CaptureDiagnostics();
            Assert.That(drained.ReliableSentPackets,
                Is.EqualTo(reliableWindow + 1));
            Assert.That(drained.SendFailures,
                Is.EqualTo(afterOlder.SendFailures));
            Assert.That(drained.ReliableSendQueueOverflows, Is.Zero);

            chunk.SnapshotTick = 3;
            Assert.That(chunk.TryWrite(payload), Is.True);
            var laterHeader = new PacketHeader
            {
                Kind = PacketKind.SnapshotChunk,
                Flags = PacketFlags.ReliableOrdered,
                PacketSequence = 3,
                ServerTick = 3
            };
            Assert.That(NetworkPacket.TryEncode(pool, laterHeader, payload,
                out var laterSnapshot), Is.True);
            Assert.That(accepted.TrySend(laterSnapshot), Is.True);
            Assert.That(laterSnapshot.Length, Is.Zero);
            server.Flush();

            var laterReceived = 0;
            WaitUntil(() =>
            {
                client.Update();
                while (client.Endpoint.TryReceive(out var packet))
                {
                    packet.Dispose();
                    laterReceived++;
                }
                client.Flush();
                server.Update();
                return laterReceived == 1;
            }, "Later snapshot was not sent after the old snapshot drained.");

            var completed = server.CaptureDiagnostics();
            Assert.That(completed.DroppedPackets,
                Is.EqualTo(afterOlder.DroppedPackets));
            Assert.That(completed.SendFailures,
                Is.EqualTo(afterOlder.SendFailures));
            Assert.That(completed.ReliableSendQueueOverflows, Is.Zero);
            Assert.That(completed.PendingReliablePackets, Is.Zero);
            Assert.That(completed.OutstandingLeases, Is.Zero);
            Assert.That(client.CaptureDiagnostics().OutstandingLeases, Is.Zero);
            Assert.That(pool.CaptureDiagnostics().OutstandingLeases, Is.Zero);
        }

        /// <summary>Verifies each channel rejects one byte above its complete packet capability.</summary>
        [TestCase(PacketFlags.ReliableOrdered, UnityTransportSettings.MaximumReliableBytes)]
        [TestCase(PacketFlags.UnreliableSequenced, UnityTransportLimits.MaximumUnreliableBytes)]
        public void PacketAboveChannelLimitIsRejectedAndConsumed(PacketFlags flags,
            int limit)
        {
            var settings = Settings(ReservePort());
            using var server = new UnityTransportServerHost(settings);
            using var client = new UnityTransportClientHost(settings);
            WaitForConnection(server, client);
            using var pool = new NetworkBufferPool(256 * 1024);
            var packet = Packet(pool, flags, limit + 1);

            Assert.That(client.Endpoint.TrySend(packet), Is.False);
            Assert.That(packet.Length, Is.Zero);
            Assert.That(pool.CaptureDiagnostics().OutstandingLeases, Is.Zero);
            Assert.That(client.CaptureDiagnostics().SendFailures, Is.EqualTo(1));
        }

        /// <summary>Verifies reliable backpressure is bounded and endpoint disposal releases caller leases.</summary>
        [Test]
        public void ReliableQueueOverflowIsBoundedAndDisposed()
        {
            var settings = Settings(ReservePort());
            using var server = new UnityTransportServerHost(settings);
            using var client = new UnityTransportClientHost(settings);
            WaitForConnection(server, client);
            using var pool = new NetworkBufferPool(256 * 1024);

            var first = Packet(pool, PacketFlags.ReliableOrdered,
                UnityTransportSettings.MaximumReliableBytes);
            Assert.That(client.Endpoint.TrySend(first), Is.True);
            var deferred = Packet(pool, PacketFlags.ReliableOrdered,
                UnityTransportSettings.MaximumReliableBytes);
            Assert.That(client.Endpoint.TrySend(deferred), Is.True);
            var overflowed = false;
            for (var index = 0; index < ProtocolLimits.MaxChunkMappings; index++)
            {
                var packet = Packet(pool, PacketFlags.ReliableOrdered,
                    PacketHeader.Size);
                if (client.Endpoint.TrySend(packet))
                    continue;
                Assert.That(packet.Length, Is.Zero);
                overflowed = true;
                break;
            }

            Assert.That(overflowed, Is.True);
            var diagnostics = client.CaptureDiagnostics();
            Assert.That(diagnostics.PendingReliablePackets, Is.GreaterThan(0));
            Assert.That(diagnostics.PendingReliablePackets,
                Is.LessThan(ProtocolLimits.MaxChunkMappings));
            Assert.That(diagnostics.PendingReliablePacketsHighWater,
                Is.EqualTo(diagnostics.PendingReliablePackets));
            Assert.That(diagnostics.PendingReliableBytesHighWater,
                Is.EqualTo(diagnostics.PendingReliableBytes));
            Assert.That(diagnostics.ReliableSendQueueOverflows, Is.EqualTo(1));
            Assert.That(diagnostics.DroppedPackets, Is.EqualTo(1));
            Assert.That(diagnostics.SendFailures, Is.EqualTo(1));
            Assert.That(pool.CaptureDiagnostics().OutstandingLeases,
                Is.EqualTo(diagnostics.PendingReliablePackets));

            client.Endpoint.Dispose();
            Assert.That(client.CaptureDiagnostics().PendingReliablePackets,
                Is.Zero);
            Assert.That(pool.CaptureDiagnostics().OutstandingLeases, Is.Zero);
        }

        /// <summary>Verifies one reliable window per client fits the server native send queue.</summary>
        [Test]
        public void ServerReliableFanoutFitsNativeSendQueue()
        {
            const int clientCount = 64;
            const int reliableWindow = 64;
            var settings = Settings(ReservePort());
            settings.MaximumConnections = clientCount;
            using var server = new UnityTransportServerHost(settings);
            var clients = new UnityTransportClientHost[clientCount];
            var accepted = new INetworkTransport[clientCount];
            var received = new int[clientCount];
            var acceptedCount = 0;
            using var pool = new NetworkBufferPool(256 * 1024);

            try
            {
                for (var index = 0; index < clientCount; index++)
                    clients[index] = new UnityTransportClientHost(settings);

                WaitUntil(() =>
                {
                    for (var index = 0; index < clientCount; index++)
                        clients[index].Update();
                    server.Update();
                    while (server.TryAccept(out var endpoint))
                        accepted[acceptedCount++] = endpoint;
                    if (acceptedCount != clientCount)
                        return false;
                    for (var index = 0; index < clientCount; index++)
                        if (!clients[index].Connected)
                            return false;
                    return true;
                }, "UTP reliable fanout clients were not connected.");

                for (var clientIndex = 0; clientIndex < clientCount; clientIndex++)
                {
                    for (var packetIndex = 0; packetIndex < reliableWindow; packetIndex++)
                    {
                        var header = new PacketHeader
                        {
                            Kind = PacketKind.Ping,
                            Flags = PacketFlags.ReliableOrdered,
                        };
                        Assert.That(NetworkPacket.TryEncode(pool, header,
                            ReadOnlySpan<byte>.Empty, out var packet), Is.True);
                        Assert.That(accepted[clientIndex].TrySend(packet), Is.True);
                    }
                }

                var submitted = server.CaptureDiagnostics();
                Assert.That(submitted.ReliableSentPackets,
                    Is.EqualTo(clientCount * reliableWindow));
                Assert.That(submitted.PendingReliablePackets, Is.Zero);
                Assert.That(submitted.PendingReliablePacketsHighWater, Is.Zero);
                server.Flush();
                WaitUntil(() =>
                {
                    for (var index = 0; index < clientCount; index++)
                    {
                        clients[index].Update();
                        while (clients[index].Endpoint.TryReceive(out var packet))
                        {
                            packet.Dispose();
                            received[index]++;
                        }
                    }
                    server.Update();
                    for (var index = 0; index < clientCount; index++)
                        if (received[index] != reliableWindow)
                            return false;
                    return true;
                }, "UTP reliable fanout packets were not received.");

                var diagnostics = server.CaptureDiagnostics();
                Assert.That(diagnostics.SendFailures, Is.Zero);
                Assert.That(diagnostics.ReliableSentPackets,
                    Is.EqualTo(clientCount * reliableWindow));
                Assert.That(diagnostics.PendingReliablePackets, Is.Zero);
                Assert.That(diagnostics.OutstandingLeases, Is.Zero);
                Assert.That(pool.CaptureDiagnostics().OutstandingLeases, Is.Zero);
                for (var index = 0; index < clientCount; index++)
                    Assert.That(clients[index].CaptureDiagnostics().OutstandingLeases,
                        Is.Zero);
            }
            finally
            {
                for (var index = 0; index < clientCount; index++)
                    clients[index]?.Dispose();
            }
        }

        /// <summary>Verifies warm send, update and receive allocate no managed memory.</summary>
        [Test]
        public void WarmTransferAllocatesNoManagedMemory()
        {
            var settings = Settings(ReservePort());
            using var server = new UnityTransportServerHost(settings);
            using var client = new UnityTransportClientHost(settings);
            var accepted = WaitForConnection(server, client);
            using var pool = new NetworkBufferPool(512 * 1024);

            for (var index = 0; index < 64; index++)
                Assert.That(TransferPacket(server, client, accepted, pool), Is.True);
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
            var before = GC.GetAllocatedBytesForCurrentThread();
            var completed = true;
            for (var index = 0; index < 256; index++)
                completed &= TransferPacket(server, client, accepted, pool);
            var allocated = GC.GetAllocatedBytesForCurrentThread() - before;

            Assert.That(completed, Is.True);
            Assert.That(allocated, Is.Zero);

            var reliablePayload = new byte[
                UnityTransportSettings.MaximumReliableBytes - PacketHeader.Size];
            for (var index = 0; index < 16; index++)
                Assert.That(TransferReliablePair(server, client, accepted, pool,
                    reliablePayload), Is.True);
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
            before = GC.GetAllocatedBytesForCurrentThread();
            completed = true;
            for (var index = 0; index < 64; index++)
                completed &= TransferReliablePair(server, client, accepted, pool,
                    reliablePayload);
            allocated = GC.GetAllocatedBytesForCurrentThread() - before;

            Assert.That(completed, Is.True);
            Assert.That(allocated, Is.Zero);
        }

        /// <summary>Verifies the server rejects connections beyond its configured bound.</summary>
        [Test]
        public void ServerEnforcesMaximumConnections()
        {
            var settings = Settings(ReservePort());
            settings.MaximumConnections = 1;
            using var server = new UnityTransportServerHost(settings);
            using var first = new UnityTransportClientHost(settings);
            using var second = new UnityTransportClientHost(settings);

            for (var attempt = 0; attempt < 500; attempt++)
            {
                first.Update();
                second.Update();
                server.Update();
                if (server.CaptureDiagnostics().DroppedPackets > 0)
                    break;
                Thread.Sleep(1);
            }

            var accepted = 0;
            while (server.TryAccept(out _))
                accepted++;
            var diagnostics = server.CaptureDiagnostics();
            Assert.That(accepted, Is.EqualTo(1));
            Assert.That(diagnostics.Connections, Is.EqualTo(1));
            Assert.That(diagnostics.DroppedPackets, Is.GreaterThanOrEqualTo(1));
        }

        /// <summary>Verifies an endpoint disconnected before admission is never returned later.</summary>
        [Test]
        public void TryAcceptPrunesDisconnectedEndpoint()
        {
            var settings = Settings(ReservePort());
            using var server = new UnityTransportServerHost(settings);
            using var client = new UnityTransportClientHost(settings);

            WaitUntil(() =>
            {
                client.Update();
                server.Update();
                return client.Connected && server.CaptureDiagnostics().Connections == 1;
            }, "Connection was not accepted by the driver.");

            client.Endpoint.Dispose();
            client.Flush();
            WaitUntil(() =>
            {
                client.Update();
                server.Update();
                return server.CaptureDiagnostics().Connections == 0;
            }, "Server did not observe the disconnect.");

            Assert.That(server.TryAccept(out _), Is.False);
        }

        /// <summary>Verifies a remote disconnect publishes its exact connection once.</summary>
        [Test]
        public void RemoteDisconnectPublishesExactConnectionOnce()
        {
            var settings = Settings(ReservePort());
            using var server = new UnityTransportServerHost(settings);
            using var client = new UnityTransportClientHost(settings);
            var accepted = WaitForConnection(server, client);

            client.Endpoint.Dispose();
            client.Flush();
            WaitUntil(() =>
            {
                client.Update();
                server.Update();
                return server.CaptureDiagnostics().Connections == 0;
            }, "Server did not observe the disconnect.");

            Assert.That(server.TryDequeueDisconnected(out var disconnected), Is.True);
            Assert.That(disconnected, Is.EqualTo(accepted.Connection));
            Assert.That(server.TryDequeueDisconnected(out _), Is.False);
        }

        /// <summary>Verifies local server endpoint disposal does not publish a disconnect notification.</summary>
        [Test]
        public void LocalServerEndpointDisposeDoesNotPublishDisconnect()
        {
            var settings = Settings(ReservePort());
            using var server = new UnityTransportServerHost(settings);
            using var client = new UnityTransportClientHost(settings);
            var accepted = WaitForConnection(server, client);

            accepted.Dispose();
            server.Flush();
            WaitUntil(() =>
            {
                server.Update();
                client.Update();
                return !client.Connected;
            }, "Client did not observe the locally disposed server endpoint.");

            Assert.That(server.TryDequeueDisconnected(out _), Is.False);
        }

        /// <summary>Verifies remote disconnect notifications preserve observation order.</summary>
        [Test]
        public void RemoteDisconnectsPreserveFifoOrderForTwoClients()
        {
            var settings = Settings(ReservePort());
            settings.MaximumConnections = 2;
            using var server = new UnityTransportServerHost(settings);
            using var firstClient = new UnityTransportClientHost(settings);
            using var secondClient = new UnityTransportClientHost(settings);
            var firstAccepted = WaitForConnection(server, firstClient);
            var secondAccepted = WaitForConnection(server, secondClient);

            DisconnectClient(server, firstClient, 1, 1);
            DisconnectClient(server, secondClient, 0, 2);

            Assert.That(server.TryDequeueDisconnected(out var firstDisconnected), Is.True);
            Assert.That(firstDisconnected, Is.EqualTo(firstAccepted.Connection));
            Assert.That(server.TryDequeueDisconnected(out var secondDisconnected), Is.True);
            Assert.That(secondDisconnected, Is.EqualTo(secondAccepted.Connection));
            Assert.That(server.TryDequeueDisconnected(out _), Is.False);
        }

        /// <summary>Verifies bounded disconnect storage drops newest overflow events diagnostically.</summary>
        [Test]
        public void DisconnectedQueueDropsNewestWhenBoundedStorageOverflows()
        {
            var settings = Settings(ReservePort());
            settings.MaximumConnections = 1;
            using var server = new UnityTransportServerHost(settings);
            ConnectionId firstConnection;

            using (var firstClient = new UnityTransportClientHost(settings))
            {
                var firstAccepted = WaitForConnection(server, firstClient);
                firstConnection = firstAccepted.Connection;
                DisconnectClient(server, firstClient, 0, 1);
            }

            var droppedBeforeSecondDisconnect = server.CaptureDiagnostics().DroppedPackets;
            using (var secondClient = new UnityTransportClientHost(settings))
            {
                WaitForConnection(server, secondClient);
                DisconnectClient(server, secondClient, 0, 2);
            }

            var diagnostics = server.CaptureDiagnostics();
            Assert.That(diagnostics.DroppedPackets, Is.GreaterThan(droppedBeforeSecondDisconnect));
            Assert.That(server.TryDequeueDisconnected(out var pending), Is.True);
            Assert.That(pending, Is.EqualTo(firstConnection));
            Assert.That(server.TryDequeueDisconnected(out _), Is.False);
        }

        /// <summary>Verifies reliable receive overflow disconnects the overloaded peer and releases its leases.</summary>
        [Test]
        public void ReliableReceiveQueueOverflowDisconnectsPeerAndReleasesLeases()
        {
            var settings = Settings(ReservePort());
            settings.ReceiveQueueCapacity = 1;
            using var server = new UnityTransportServerHost(settings);
            using var client = new UnityTransportClientHost(settings);
            var accepted = WaitForConnection(server, client);
            using var pool = new NetworkBufferPool(4096);

            Assert.That(client.Endpoint.TrySend(Packet(pool,
                PacketFlags.ReliableOrdered, PacketHeader.Size)), Is.True);
            Assert.That(client.Endpoint.TrySend(Packet(pool,
                PacketFlags.ReliableOrdered, PacketHeader.Size)), Is.True);
            client.Flush();
            WaitUntil(() =>
            {
                client.Update();
                server.Update();
                var diagnostics = server.CaptureDiagnostics();
                return diagnostics.ReceiveQueueOverflows > 0 &&
                    diagnostics.Connections == 0;
            }, "Receive queue did not report overflow.");

            var diagnostics = server.CaptureDiagnostics();
            Assert.That(diagnostics.QueuedPackets, Is.Zero);
            Assert.That(diagnostics.ReceiveQueueOverflows, Is.EqualTo(1));
            Assert.That(diagnostics.OutstandingLeases, Is.Zero);
            Assert.That(accepted.TryReceive(out _), Is.False);
            Assert.That(diagnostics.Disconnects, Is.EqualTo(1));
            Assert.That(server.TryDequeueDisconnected(out var disconnected), Is.True);
            Assert.That(disconnected, Is.EqualTo(accepted.Connection));
            Assert.That(server.TryDequeueDisconnected(out _), Is.False);
            WaitUntil(() =>
            {
                client.Update();
                return !client.Connected;
            }, "Overloaded peer did not observe the local disconnect.");
            server.Update();
            Assert.That(server.TryDequeueDisconnected(out _), Is.False);
            Assert.That(server.CaptureDiagnostics().OutstandingLeases, Is.Zero);
            Assert.That(pool.CaptureDiagnostics().OutstandingLeases, Is.Zero);
        }

        /// <summary>Verifies reliable overflow isolates one peer, preserves another, and permits a new epoch.</summary>
        [Test]
        public void ReliableReceiveOverflowIsolatesPeerAndAllowsReconnect()
        {
            var settings = Settings(ReservePort());
            settings.MaximumConnections = 2;
            settings.ReceiveQueueCapacity = 1;
            using var server = new UnityTransportServerHost(settings);
            using var firstClient = new UnityTransportClientHost(settings);
            using var secondClient = new UnityTransportClientHost(settings);
            var firstAccepted = WaitForConnection(server, firstClient);
            var secondAccepted = WaitForConnection(server, secondClient);
            using var pool = new NetworkBufferPool(4096);

            Assert.That(firstClient.Endpoint.TrySend(Packet(pool,
                PacketFlags.ReliableOrdered, PacketHeader.Size)), Is.True);
            Assert.That(firstClient.Endpoint.TrySend(Packet(pool,
                PacketFlags.ReliableOrdered, PacketHeader.Size)), Is.True);
            Assert.That(secondClient.Endpoint.TrySend(Packet(pool,
                PacketFlags.ReliableOrdered, PacketHeader.Size)), Is.True);
            firstClient.Flush();
            secondClient.Flush();

            WaitUntil(() =>
            {
                firstClient.Update();
                secondClient.Update();
                server.Update();
                var diagnostics = server.CaptureDiagnostics();
                return diagnostics.ReceiveQueueOverflows > 0 &&
                    diagnostics.Connections == 1 && !firstClient.Connected &&
                    secondClient.Connected;
            }, "Reliable overflow did not isolate the overloaded peer.");

            var diagnostics = server.CaptureDiagnostics();
            Assert.That(diagnostics.ReceiveQueueOverflows, Is.EqualTo(1));
            Assert.That(diagnostics.QueuedPackets, Is.EqualTo(1));
            Assert.That(firstAccepted.TryReceive(out _), Is.False);
            Assert.That(diagnostics.Disconnects, Is.EqualTo(1));
            Assert.That(server.TryDequeueDisconnected(out var disconnected), Is.True);
            Assert.That(disconnected, Is.EqualTo(firstAccepted.Connection));
            Assert.That(server.TryDequeueDisconnected(out _), Is.False);
            Assert.That(secondAccepted.TryReceive(out var secondPacket), Is.True);
            secondPacket.Dispose();
            server.Update();
            Assert.That(server.TryDequeueDisconnected(out _), Is.False);
            Assert.That(server.CaptureDiagnostics().OutstandingLeases, Is.Zero);
            Assert.That(pool.CaptureDiagnostics().OutstandingLeases, Is.Zero);

            using var reconnectedClient = new UnityTransportClientHost(settings);
            var reconnected = WaitForConnection(server, reconnectedClient);
            Assert.That(reconnected.Connection, Is.Not.EqualTo(firstAccepted.Connection));

            var header = new PacketHeader
            {
                Kind = PacketKind.Ping,
                Flags = PacketFlags.ReliableOrdered,
                SessionEpoch = 2,
                PacketSequence = 1,
            };
            Assert.That(NetworkPacket.TryEncode(pool, header,
                ReadOnlySpan<byte>.Empty, out var reconnectPacket), Is.True);
            Assert.That(reconnectedClient.Endpoint.TrySend(reconnectPacket), Is.True);
            reconnectedClient.Flush();
            var received = WaitForPacket(server, reconnectedClient, reconnected);
            try
            {
                Assert.That(NetworkPacket.TryDecode(received, out var receivedHeader,
                    out _), Is.True);
                Assert.That(receivedHeader.SessionEpoch, Is.EqualTo(2));
            }
            finally
            {
                received.Dispose();
            }
            Assert.That(server.CaptureDiagnostics().OutstandingLeases, Is.Zero);
            Assert.That(pool.CaptureDiagnostics().OutstandingLeases, Is.Zero);
        }

        /// <summary>Verifies unreliable receive overflow drops excess data without disconnecting its peer.</summary>
        [Test]
        public void UnreliableReceiveQueueOverflowDropsPacketWithoutDisconnect()
        {
            var settings = Settings(ReservePort());
            settings.ReceiveQueueCapacity = 1;
            using var server = new UnityTransportServerHost(settings);
            using var client = new UnityTransportClientHost(settings);
            var accepted = WaitForConnection(server, client);
            using var pool = new NetworkBufferPool(4096);

            Assert.That(client.Endpoint.TrySend(Packet(pool,
                PacketFlags.UnreliableSequenced, PacketHeader.Size)), Is.True);
            Assert.That(client.Endpoint.TrySend(Packet(pool,
                PacketFlags.UnreliableSequenced, PacketHeader.Size)), Is.True);
            client.Flush();
            WaitUntil(() =>
            {
                client.Update();
                server.Update();
                return server.CaptureDiagnostics().ReceiveQueueOverflows > 0;
            }, "Unreliable receive queue did not report overflow.");

            var diagnostics = server.CaptureDiagnostics();
            Assert.That(diagnostics.Connections, Is.EqualTo(1));
            Assert.That(diagnostics.QueuedPackets, Is.EqualTo(1));
            Assert.That(diagnostics.ReceiveQueueOverflows, Is.EqualTo(1));
            Assert.That(diagnostics.Disconnects, Is.Zero);
            Assert.That(server.TryDequeueDisconnected(out _), Is.False);
            Assert.That(accepted.TryReceive(out var received), Is.True);
            received.Dispose();
            Assert.That(client.Connected, Is.True);
            Assert.That(server.CaptureDiagnostics().OutstandingLeases, Is.Zero);
            Assert.That(pool.CaptureDiagnostics().OutstandingLeases, Is.Zero);
        }

        /// <summary>Verifies raw invalid transport data increments malformed and dropped counters.</summary>
        [Test]
        public void MalformedRawPacketIsRejectedAndDiagnosed()
        {
            var settings = Settings(ReservePort());
            using var server = new UnityTransportServerHost(settings);
            using var raw = NetworkDriver.Create();
            var connection = raw.Connect(NetworkEndpoint.LoopbackIpv4.WithPort(settings.Port));

            WaitUntil(() =>
            {
                raw.ScheduleUpdate().Complete();
                server.Update();
                return connection.GetState(raw) == NetworkConnection.State.Connected;
            }, "Raw UTP connection was not established.");

            while (connection.PopEvent(raw, out _, out _) != NetworkEvent.Type.Empty)
            {
                // Drain the raw driver's connect event before sending malformed data.
            }

            raw.BeginSend(connection, out var writer);
            writer.WriteByte(0xFF);
            Assert.That(raw.EndSend(writer), Is.GreaterThanOrEqualTo(0));
            raw.ScheduleFlushSend(default).Complete();

            WaitUntil(() =>
            {
                raw.ScheduleUpdate().Complete();
                server.Update();
                return server.CaptureDiagnostics().MalformedPackets > 0;
            }, "Malformed raw packet was not diagnosed.");

            var diagnostics = server.CaptureDiagnostics();
            Assert.That(diagnostics.MalformedPackets, Is.GreaterThanOrEqualTo(1));
            Assert.That(diagnostics.DroppedPackets, Is.GreaterThanOrEqualTo(1));
            Assert.That(diagnostics.OutstandingLeases, Is.Zero);

            while (connection.PopEvent(raw, out _, out _) != NetworkEvent.Type.Empty)
            {
                // Drain the raw driver's pending disconnect/data events before disposal.
            }
        }

        /// <summary>Verifies a failed listener construction releases native ownership.</summary>
        [Test]
        public void FailedListenDoesNotPoisonLaterConstruction()
        {
            var settings = Settings(ReservePort());
            using (var first = new UnityTransportServerHost(settings))
            {
                LogAssert.Expect(UnityEngine.LogType.Error,
                    new Regex(@"^(Failed to bind UDP socket|Baselib operation failed\. Failed to create UDP socket)"));
                Assert.Throws<InvalidOperationException>(() =>
                    new UnityTransportServerHost(settings));
            }
            using var later = new UnityTransportServerHost(settings);
            Assert.That(later.CaptureDiagnostics().Connections, Is.Zero);
        }

        private static INetworkTransport WaitForConnection(
            UnityTransportServerHost server, UnityTransportClientHost client)
        {
            INetworkTransport accepted = null;
            WaitUntil(() =>
            {
                client.Update();
                server.Update();
                if (accepted == null)
                    server.TryAccept(out accepted);
                return client.Connected && accepted != null;
            }, "UTP loopback connection was not established.");
            return accepted;
        }

        private static NetworkBufferLease WaitForPacket(UnityTransportServerHost server,
            UnityTransportClientHost client, INetworkTransport endpoint)
        {
            NetworkBufferLease received = null;
            WaitUntil(() =>
            {
                client.Update();
                server.Update();
                return endpoint.TryReceive(out received);
            }, "UTP loopback packet was not received.");
            return received;
        }

        private static bool TransferPacket(UnityTransportServerHost server,
            UnityTransportClientHost client, INetworkTransport endpoint,
            NetworkBufferPool pool)
        {
            var header = new PacketHeader
            {
                Kind = PacketKind.Ping,
                Flags = PacketFlags.UnreliableSequenced,
            };
            if (!NetworkPacket.TryEncode(pool, header, ReadOnlySpan<byte>.Empty,
                    out var packet) || !client.Endpoint.TrySend(packet))
                return false;
            client.Flush();
            for (var attempt = 0; attempt < 32; attempt++)
            {
                client.Update();
                server.Update();
                if (!endpoint.TryReceive(out var received))
                    continue;
                received.Dispose();
                return true;
            }
            return false;
        }

        private static bool TransferReliablePair(
            UnityTransportServerHost server, UnityTransportClientHost client,
            INetworkTransport endpoint, NetworkBufferPool pool, byte[] payload)
        {
            var header = new PacketHeader
            {
                Kind = PacketKind.Ping,
                Flags = PacketFlags.ReliableOrdered
            };
            if (!NetworkPacket.TryEncode(pool, header, payload,
                    out var first) || !client.Endpoint.TrySend(first) ||
                !NetworkPacket.TryEncode(pool, header, payload,
                    out var second) || !client.Endpoint.TrySend(second))
                return false;
            client.Flush();
            var received = 0;
            for (var attempt = 0; attempt < 256; attempt++)
            {
                client.Update();
                server.Update();
                while (endpoint.TryReceive(out var packet))
                {
                    packet.Dispose();
                    received++;
                }
                if (received == 2)
                    return true;
                if (received > 2)
                    return false;
            }
            return false;
        }

        private static void DisconnectClient(UnityTransportServerHost server,
            UnityTransportClientHost client, int expectedConnections, long expectedDisconnects)
        {
            client.Endpoint.Dispose();
            client.Flush();
            WaitUntil(() =>
            {
                client.Update();
                server.Update();
                var diagnostics = server.CaptureDiagnostics();
                return diagnostics.Connections == expectedConnections &&
                    diagnostics.Disconnects >= expectedDisconnects;
            }, "Server did not observe the disconnect.");
        }

        private static void WaitUntil(Func<bool> condition, string message)
        {
            for (var attempt = 0; attempt < 1_000; attempt++)
            {
                if (condition())
                    return;
                Thread.Sleep(1);
            }
            Assert.Fail(message);
        }

        private static NetworkBufferLease Packet(NetworkBufferPool pool, PacketFlags flags,
            int packetBytes)
        {
            var payload = new byte[packetBytes - PacketHeader.Size];
            var header = new PacketHeader { Kind = PacketKind.Ping, Flags = flags };
            Assert.That(NetworkPacket.TryEncode(pool, header, payload, out var packet), Is.True);
            return packet;
        }

        private static UnityTransportSettings Settings(ushort port)
        {
            var settings = UnityTransportSettings.Default;
            settings.Address = "127.0.0.1";
            settings.Port = port;
            return settings;
        }

        private static ushort ReservePort()
        {
            using var socket = new UdpClient(new IPEndPoint(IPAddress.Loopback, 0));
            return checked((ushort)((IPEndPoint)socket.Client.LocalEndPoint).Port);
        }
    }
}
