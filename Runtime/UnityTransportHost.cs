namespace UniGame.StaticEcs.Network.UnityTransport
{
    using System;
    using System.Collections.Generic;
    using Unity.Networking.Transport;
    using Unity.Networking.Transport.Utilities;

    /// <summary>Owns one client-side Unity Transport driver and exact-packet endpoint.</summary>
    public sealed class UnityTransportClientHost : IDisposable
    {
        private readonly UnityTransportDriver _driver;

        public UnityTransportClientHost(UnityTransportSettings settings)
        {
            var normalized = settings.Normalize(false);
            UnityTransportDriver driver = null;
            try
            {
                driver = new UnityTransportDriver(in normalized, false);
                Endpoint = driver.Connect(NetworkEndpoint.Parse(normalized.Address, normalized.Port));
                _driver = driver;
            }
            catch
            {
                driver?.Dispose();
                throw;
            }
        }

        /// <summary>Gets the protocol-facing client endpoint.</summary>
        public INetworkTransport Endpoint { get; }
        /// <summary>Gets whether the underlying UTP connection is established.</summary>
        public bool Connected => _driver.Connected;
        /// <summary>Advances driver jobs and transfers received packets to bounded queues.</summary>
        public void Update() => _driver.Update();
        /// <summary>Completes pending send jobs.</summary>
        public void Flush() => _driver.Flush();
        public UnityTransportDiagnostics CaptureDiagnostics() => _driver.CaptureDiagnostics();
        public void Dispose() => _driver.Dispose();
    }

    /// <summary>Owns one listening Unity Transport driver and accepted exact-packet endpoints.</summary>
    public sealed class UnityTransportServerHost : IDisposable
    {
        private readonly UnityTransportDriver _driver;

        public UnityTransportServerHost(UnityTransportSettings settings)
        {
            var normalized = settings.Normalize(true);
            UnityTransportDriver driver = null;
            try
            {
                driver = new UnityTransportDriver(in normalized, true);
                driver.Listen(NetworkEndpoint.Parse(normalized.Address, normalized.Port));
                _driver = driver;
            }
            catch
            {
                driver?.Dispose();
                throw;
            }
        }

        /// <summary>Advances accept, disconnect and receive processing.</summary>
        public void Update() => _driver.Update();
        /// <summary>Returns the next newly accepted protocol-facing endpoint.</summary>
        public bool TryAccept(out INetworkTransport endpoint) => _driver.TryAccept(out endpoint);
        /// <summary>Returns the next remote connection observed as disconnected.</summary>
        public bool TryDequeueDisconnected(out ConnectionId connection) =>
            _driver.TryDequeueDisconnected(out connection);
        /// <summary>Completes pending send jobs.</summary>
        public void Flush() => _driver.Flush();
        public UnityTransportDiagnostics CaptureDiagnostics() => _driver.CaptureDiagnostics();
        public void Dispose() => _driver.Dispose();
    }

    internal sealed class UnityTransportDriver : IDisposable
    {
        private const int NetworkMessageSize = 1472;
        private const int ReliableWindowSize = 64;
        // 128 default connections multiplied by one reliable window per connection.
        private const int ServerSendQueueCapacity = 8192;
        // UTP 2.6 adds a 2-byte fragmentation header and a 16-byte reliable header
        // at window 64. Keep that internal overhead outside the public 64 KiB limit.
        private const int ReliableFragmentationPipelineHeaderBytes = 18;
        private const int ReliableControlReserve = 8;
        private const int ReliableSnapshotBodyBytes =
            UnityTransportSettings.MaximumReliableBytes - PacketHeader.Size -
            SnapshotChunkHeader.Size;
        private const int ReliableSendQueueCapacity =
            (ProtocolLimits.MaxDecodedPayloadBytes + ReliableSnapshotBodyBytes - 1) /
            ReliableSnapshotBodyBytes + ReliableControlReserve;
        private const int SendQueueFull = (int)Unity.Networking.Transport.Error.StatusCode.NetworkSendQueueFull;

        private readonly Dictionary<NetworkConnection, UnityTransportEndpoint> _connections =
            new Dictionary<NetworkConnection, UnityTransportEndpoint>();
        private readonly Queue<UnityTransportEndpoint> _accepted =
            new Queue<UnityTransportEndpoint>();
        private readonly Queue<ConnectionId> _disconnected;
        private readonly NetworkBufferPool _pool;
        private readonly UnityTransportSettings _settings;
        private readonly NetworkConnection[] _removedConnections;
        private NetworkDriver _driver;
        private NetworkPipeline _reliable;
        private NetworkPipeline _unreliable;
        private uint _nextConnection;
        private bool _listener;
        private bool _disposed;
        private long _received;
        private long _reliableReceivedPackets;
        private long _reliableReceivedBytes;
        private long _unreliableReceivedPackets;
        private long _unreliableReceivedBytes;
        private long _sent;
        private long _reliableSentPackets;
        private long _reliableSentBytes;
        private long _unreliableSentPackets;
        private long _unreliableSentBytes;
        private long _dropped;
        private long _receiveQueueOverflows;
        private long _malformedPackets;
        private long _sendFailures;
        private long _disconnects;
        private int _pendingReliablePackets;
        private long _pendingReliableBytes;
        private int _pendingReliablePacketsHighWater;
        private long _pendingReliableBytesHighWater;
        private long _reliableSendQueueOverflows;

        internal UnityTransportDriver(in UnityTransportSettings settings, bool listener)
        {
            _settings = settings;
            _listener = listener;
            _removedConnections = new NetworkConnection[_settings.MaximumConnections];
            _disconnected = new Queue<ConnectionId>(_settings.MaximumConnections);
            _pool = new NetworkBufferPool(listener
                ? NetworkBufferPool.DefaultServerRetainedBytes
                : NetworkBufferPool.DefaultClientRetainedBytes);
            var networkSettings = new NetworkSettings();
            try
            {
                if (_listener)
                {
                    networkSettings.WithNetworkConfigParameters(
                        maxMessageSize: NetworkMessageSize,
                        sendQueueCapacity: ServerSendQueueCapacity);
                }
                else
                {
                    networkSettings.WithNetworkConfigParameters(
                        maxMessageSize: NetworkMessageSize);
                }
                networkSettings.WithReliableStageParameters(windowSize: ReliableWindowSize);
                networkSettings.WithFragmentationStageParameters(
                    payloadCapacity: UnityTransportSettings.MaximumReliableBytes +
                        ReliableFragmentationPipelineHeaderBytes);
                _driver = NetworkDriver.Create(networkSettings);
                _reliable = _driver.CreatePipeline(typeof(FragmentationPipelineStage),
                    typeof(ReliableSequencedPipelineStage));
                _unreliable = _driver.CreatePipeline(typeof(UnreliableSequencedPipelineStage));
            }
            catch
            {
                if (_driver.IsCreated)
                    _driver.Dispose();
                _pool.Dispose();
                throw;
            }
            finally
            {
                networkSettings.Dispose();
            }
        }

        internal bool Connected
        {
            get
            {
                foreach (var endpoint in _connections.Values)
                    if (endpoint.IsConnected)
                        return true;
                return false;
            }
        }

        internal INetworkTransport Connect(NetworkEndpoint address)
        {
            ThrowIfDisposed();
            var connection = _driver.Connect(address);
            if (!connection.IsCreated)
                throw new InvalidOperationException("Unable to create a Unity Transport connection.");
            return Add(connection, false);
        }

        internal void Listen(NetworkEndpoint address)
        {
            ThrowIfDisposed();
            if (_driver.Bind(address) != 0)
                throw new InvalidOperationException($"Unable to bind Unity Transport at {address}.");
            if (_driver.Listen() != 0)
                throw new InvalidOperationException($"Unable to listen with Unity Transport at {address}.");
        }

        internal bool TryAccept(out INetworkTransport endpoint)
        {
            while (_accepted.Count > 0)
            {
                var accepted = _accepted.Dequeue();
                if (!accepted.IsDisposed && accepted.IsConnected)
                {
                    endpoint = accepted;
                    return true;
                }
            }
            endpoint = null;
            return false;
        }

        internal bool TryDequeueDisconnected(out ConnectionId connection)
        {
            if (_disconnected.Count > 0)
            {
                connection = _disconnected.Dequeue();
                return true;
            }
            connection = default;
            return false;
        }

        internal void Update()
        {
            ThrowIfDisposed();
            _driver.ScheduleUpdate().Complete();
            if (_listener)
            {
                NetworkConnection connection;
                while ((connection = _driver.Accept()) != default)
                {
                    if (_connections.Count >= _settings.MaximumConnections)
                    {
                        connection.Disconnect(_driver);
                        _dropped++;
                        continue;
                    }
                    Add(connection, true);
                }
            }

            var removedCount = 0;
            foreach (var pair in _connections)
            {
                var endpoint = pair.Value;
                NetworkEvent.Type type;
                while ((type = endpoint.NativeConnection.PopEvent(_driver, out var reader,
                           out var pipeline)) != NetworkEvent.Type.Empty)
                {
                    if (type == NetworkEvent.Type.Connect)
                    {
                        endpoint.IsConnected = true;
                        continue;
                    }
                    if (type == NetworkEvent.Type.Disconnect)
                    {
                        endpoint.IsConnected = false;
                        _disconnects++;
                        // Preserve FIFO entries when the defensive bound is exceeded; drop the newest event.
                        if (_disconnected.Count < _settings.MaximumConnections)
                            _disconnected.Enqueue(endpoint.Connection);
                        else
                            _dropped++;
                        _removedConnections[removedCount++] = pair.Key;
                        break;
                    }
                    if (type != NetworkEvent.Type.Data)
                        continue;
                    bool reliable;
                    int limit;
                    if (pipeline.Equals(_reliable))
                    {
                        reliable = true;
                        limit = UnityTransportSettings.MaximumReliableBytes;
                    }
                    else if (pipeline.Equals(_unreliable))
                    {
                        reliable = false;
                        limit = _settings.MaximumUnreliableBytes;
                    }
                    else
                    {
                        _dropped++;
                        _malformedPackets++;
                        continue;
                    }
                    if (reader.Length < PacketHeader.Size || reader.Length > limit)
                    {
                        _dropped++;
                        _malformedPackets++;
                        continue;
                    }
                    if (endpoint.QueuedPackets >= _settings.ReceiveQueueCapacity)
                    {
                        _dropped++;
                        _receiveQueueOverflows++;
                        if (reliable)
                        {
                            endpoint.IsConnected = false;
                            if (endpoint.NativeConnection.IsCreated)
                                endpoint.NativeConnection.Disconnect(_driver);
                            _removedConnections[removedCount++] = pair.Key;
                            break;
                        }
                        continue;
                    }
                    var packet = _pool.Rent(reader.Length);
                    reader.ReadBytes(packet.WritableSpan);
                    if (!NetworkPacket.TryDecode(packet, out var header, out _))
                    {
                        packet.Dispose();
                        _dropped++;
                        _malformedPackets++;
                        continue;
                    }
                    if ((header.Flags == PacketFlags.ReliableOrdered) != reliable)
                    {
                        packet.Dispose();
                        _dropped++;
                        _malformedPackets++;
                        continue;
                    }
                    endpoint.Enqueue(packet);
                    _received++;
                    if (reliable)
                    {
                        _reliableReceivedPackets++;
                        _reliableReceivedBytes += packet.Length;
                    }
                    else
                    {
                        _unreliableReceivedPackets++;
                        _unreliableReceivedBytes += packet.Length;
                    }
                }
            }

            for (var index = 0; index < removedCount; index++)
            {
                var id = _removedConnections[index];
                _removedConnections[index] = default;
                if (_connections.Remove(id, out var endpoint))
                    endpoint.DisposeFromDriver();
            }

            foreach (var endpoint in _connections.Values)
                DrainReliable(endpoint);
        }

        internal bool TrySend(UnityTransportEndpoint endpoint, NetworkBufferLease packet)
        {
            if (packet == null)
                return false;
            try
            {
                if (_disposed || !endpoint.IsConnected || packet.Length < PacketHeader.Size ||
                    !PacketHeader.TryRead(packet.Span, out var header))
                {
                    RejectSend();
                    return false;
                }
                if (!NetworkPacket.TryDecode(packet, out header, out _))
                {
                    RejectSend();
                    return false;
                }
                var reliable = header.Flags == PacketFlags.ReliableOrdered;
                if (packet.Length > (reliable
                        ? UnityTransportSettings.MaximumReliableBytes
                        : _settings.MaximumUnreliableBytes))
                {
                    RejectSend();
                    return false;
                }
                if (reliable && header.Kind == PacketKind.SnapshotChunk &&
                    endpoint.PendingSnapshotChunks != 0)
                {
                    if (header.ServerTick > endpoint.PendingSnapshotTick)
                    {
                        _dropped++;
                        return true;
                    }
                    if (header.ServerTick < endpoint.PendingSnapshotTick)
                    {
                        RejectSend();
                        return false;
                    }
                }
                if (reliable && endpoint.PendingReliablePackets != 0)
                {
                    if (!TryEnqueueReliable(endpoint, packet, in header))
                        return false;
                    packet = null;
                    return true;
                }
                var result = Submit(endpoint, packet, reliable);
                if (result >= 0)
                    return true;
                if (reliable && result == SendQueueFull)
                {
                    if (!TryEnqueueReliable(endpoint, packet, in header))
                        return false;
                    packet = null;
                    return true;
                }
                RejectSend();
                return false;
            }
            finally
            {
                packet?.Dispose();
            }
        }

        private int Submit(UnityTransportEndpoint endpoint,
            NetworkBufferLease packet, bool reliable)
        {
            var pipeline = reliable ? _reliable : _unreliable;
            var result = _driver.BeginSend(pipeline, endpoint.NativeConnection,
                out var writer, packet.Length);
            if (result != 0)
                return result;
            var span = packet.Span;
            for (var index = 0; index < span.Length; index++)
                writer.WriteByte(span[index]);
            result = _driver.EndSend(writer);
            if (result < 0)
                return result;
            _sent++;
            if (reliable)
            {
                _reliableSentPackets++;
                _reliableSentBytes += packet.Length;
            }
            else
            {
                _unreliableSentPackets++;
                _unreliableSentBytes += packet.Length;
            }
            return result;
        }

        private bool TryEnqueueReliable(UnityTransportEndpoint endpoint,
            NetworkBufferLease packet, in PacketHeader header)
        {
            if (endpoint.PendingReliablePackets >= ReliableSendQueueCapacity)
            {
                _reliableSendQueueOverflows++;
                RejectSend();
                return false;
            }
            endpoint.EnqueueReliable(packet,
                header.Kind == PacketKind.SnapshotChunk, header.ServerTick);
            _pendingReliablePackets++;
            _pendingReliableBytes += packet.Length;
            if (_pendingReliablePackets > _pendingReliablePacketsHighWater)
                _pendingReliablePacketsHighWater = _pendingReliablePackets;
            if (_pendingReliableBytes > _pendingReliableBytesHighWater)
                _pendingReliableBytesHighWater = _pendingReliableBytes;
            return true;
        }

        private void DrainReliable(UnityTransportEndpoint endpoint)
        {
            while (endpoint.TryPeekReliable(out var packet))
            {
                if (!NetworkPacket.TryDecode(packet, out var header, out _))
                {
                    endpoint.DequeueReliable();
                    ReleasePendingReliable(endpoint, packet, false);
                    packet.Dispose();
                    RejectSend();
                    continue;
                }
                var result = Submit(endpoint, packet, true);
                if (result == SendQueueFull)
                    return;
                endpoint.DequeueReliable();
                ReleasePendingReliable(endpoint, packet,
                    header.Kind == PacketKind.SnapshotChunk);
                packet.Dispose();
                if (result < 0)
                    RejectSend();
            }
        }

        internal void DisposePendingReliable(UnityTransportEndpoint endpoint)
        {
            while (endpoint.TryPeekReliable(out var packet))
            {
                PacketHeader.TryRead(packet.Span, out var header);
                endpoint.DequeueReliable();
                ReleasePendingReliable(endpoint, packet,
                    header.Kind == PacketKind.SnapshotChunk);
                packet.Dispose();
            }
        }

        private void ReleasePendingReliable(UnityTransportEndpoint endpoint,
            NetworkBufferLease packet, bool snapshot)
        {
            _pendingReliablePackets--;
            _pendingReliableBytes -= packet.Length;
            endpoint.ReleaseReliable(snapshot);
        }

        private void RejectSend()
        {
            _dropped++;
            _sendFailures++;
        }

        internal void Disconnect(UnityTransportEndpoint endpoint)
        {
            if (_disposed)
                return;
            _connections.Remove(endpoint.NativeConnection);
            if (endpoint.NativeConnection.IsCreated)
                endpoint.NativeConnection.Disconnect(_driver);
            endpoint.DisposeFromDriver();
        }

        internal void Flush()
        {
            ThrowIfDisposed();
            _driver.ScheduleFlushSend(default).Complete();
        }

        internal UnityTransportDiagnostics CaptureDiagnostics()
        {
            var queued = 0;
            foreach (var endpoint in _connections.Values)
                queued += endpoint.QueuedPackets;
            var buffers = _pool.CaptureDiagnostics();
            return new UnityTransportDiagnostics
            {
                Connections = _connections.Count,
                ReceivedPackets = _received,
                ReliableReceivedPackets = _reliableReceivedPackets,
                ReliableReceivedBytes = _reliableReceivedBytes,
                UnreliableReceivedPackets = _unreliableReceivedPackets,
                UnreliableReceivedBytes = _unreliableReceivedBytes,
                SentPackets = _sent,
                ReliableSentPackets = _reliableSentPackets,
                ReliableSentBytes = _reliableSentBytes,
                UnreliableSentPackets = _unreliableSentPackets,
                UnreliableSentBytes = _unreliableSentBytes,
                DroppedPackets = _dropped,
                ReceiveQueueOverflows = _receiveQueueOverflows,
                MalformedPackets = _malformedPackets,
                SendFailures = _sendFailures,
                Disconnects = _disconnects,
                PendingReliablePackets = _pendingReliablePackets,
                PendingReliableBytes = _pendingReliableBytes,
                PendingReliablePacketsHighWater = _pendingReliablePacketsHighWater,
                PendingReliableBytesHighWater = _pendingReliableBytesHighWater,
                ReliableSendQueueOverflows = _reliableSendQueueOverflows,
                QueuedPackets = queued,
                OutstandingLeases = buffers.OutstandingLeases,
            };
        }

        public void Dispose()
        {
            if (_disposed)
                return;
            _disposed = true;
            foreach (var endpoint in _connections.Values)
                endpoint.DisposeFromDriver();
            _connections.Clear();
            _accepted.Clear();
            _disconnected.Clear();
            try
            {
                if (_driver.IsCreated)
                    _driver.Dispose();
            }
            finally
            {
                _pool.Dispose();
            }
        }

        internal int MaxReliablePayloadBytes =>
            UnityTransportSettings.MaximumReliableBytes;

        internal int MaxUnreliablePayloadBytes =>
            _settings.MaximumUnreliableBytes;

        private UnityTransportEndpoint Add(NetworkConnection connection, bool accepted)
        {
            var id = checked(++_nextConnection);
            var endpoint = new UnityTransportEndpoint(this, connection,
                new ConnectionId(id), _settings.ReceiveQueueCapacity,
                ReliableSendQueueCapacity);
            endpoint.IsConnected = accepted;
            _connections.Add(connection, endpoint);
            if (accepted)
                _accepted.Enqueue(endpoint);
            return endpoint;
        }

        private void ThrowIfDisposed()
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(UnityTransportDriver));
        }
    }

    internal sealed class UnityTransportEndpoint : INetworkTransport
    {
        private readonly UnityTransportDriver _owner;
        private readonly Queue<NetworkBufferLease> _incoming;
        private readonly Queue<NetworkBufferLease> _pendingReliable;
        private bool _disposed;
        private int _pendingSnapshotChunks;
        private uint _pendingSnapshotTick;

        internal UnityTransportEndpoint(UnityTransportDriver owner, NetworkConnection connection,
            ConnectionId id, int receiveQueueCapacity, int reliableQueueCapacity)
        {
            _owner = owner;
            NativeConnection = connection;
            Connection = id;
            _incoming = new Queue<NetworkBufferLease>(receiveQueueCapacity);
            _pendingReliable = new Queue<NetworkBufferLease>(reliableQueueCapacity);
        }

        public ConnectionId Connection { get; }
        internal NetworkConnection NativeConnection { get; }
        internal bool IsConnected { get; set; }
        internal bool IsDisposed => _disposed;
        internal int QueuedPackets => _incoming.Count;
        internal int PendingReliablePackets => _pendingReliable.Count;
        internal int PendingSnapshotChunks => _pendingSnapshotChunks;
        internal uint PendingSnapshotTick => _pendingSnapshotTick;

        public int MaxReliablePayloadBytes => _owner.MaxReliablePayloadBytes;
        public int MaxUnreliablePayloadBytes => _owner.MaxUnreliablePayloadBytes;
        public bool TrySend(NetworkBufferLease packet) => _owner.TrySend(this, packet);

        public bool TryReceive(out NetworkBufferLease packet)
        {
            if (!_disposed && _incoming.Count > 0)
            {
                packet = _incoming.Dequeue();
                return true;
            }
            packet = null;
            return false;
        }

        public void Dispose()
        {
            if (_disposed)
                return;
            _owner.Disconnect(this);
        }

        internal void Enqueue(NetworkBufferLease packet)
        {
            if (_disposed)
            {
                packet.Dispose();
                return;
            }
            _incoming.Enqueue(packet);
        }

        internal void EnqueueReliable(NetworkBufferLease packet,
            bool snapshot, uint snapshotTick)
        {
            _pendingReliable.Enqueue(packet);
            if (!snapshot)
                return;
            if (_pendingSnapshotChunks == 0)
                _pendingSnapshotTick = snapshotTick;
            _pendingSnapshotChunks++;
        }

        internal bool TryPeekReliable(out NetworkBufferLease packet)
        {
            if (_pendingReliable.Count == 0)
            {
                packet = null;
                return false;
            }
            packet = _pendingReliable.Peek();
            return true;
        }

        internal void DequeueReliable() => _pendingReliable.Dequeue();

        internal void ReleaseReliable(bool snapshot)
        {
            if (!snapshot)
                return;
            _pendingSnapshotChunks--;
            if (_pendingSnapshotChunks == 0)
                _pendingSnapshotTick = 0;
        }

        internal void DisposeFromDriver()
        {
            if (_disposed)
                return;
            _disposed = true;
            IsConnected = false;
            while (_incoming.Count > 0)
                _incoming.Dequeue().Dispose();
            _owner.DisposePendingReliable(this);
        }
    }
}
