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

        /// <summary>Creates and starts a connection to the configured endpoint.</summary>
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
        /// <summary>Captures current counters.</summary>
        public UnityTransportDiagnostics CaptureDiagnostics() => _driver.CaptureDiagnostics();
        /// <inheritdoc />
        public void Dispose() => _driver.Dispose();
    }

    /// <summary>Owns one listening Unity Transport driver and accepted exact-packet endpoints.</summary>
    public sealed class UnityTransportServerHost : IDisposable
    {
        private readonly UnityTransportDriver _driver;

        /// <summary>Creates and starts a listener at the configured endpoint.</summary>
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
        /// <summary>Captures current counters.</summary>
        public UnityTransportDiagnostics CaptureDiagnostics() => _driver.CaptureDiagnostics();
        /// <inheritdoc />
        public void Dispose() => _driver.Dispose();
    }

    internal sealed class UnityTransportDriver : IDisposable
    {
        private const int NetworkMessageSize = 1472;
        private const int ReliableWindowSize = 64;
        // UTP 2.6 adds a 2-byte fragmentation header and a 16-byte reliable header
        // at window 64. Keep that internal overhead outside the public 64 KiB limit.
        private const int ReliableFragmentationPipelineHeaderBytes = 18;

        private readonly Dictionary<NetworkConnection, UnityTransportEndpoint> _connections =
            new Dictionary<NetworkConnection, UnityTransportEndpoint>();
        private readonly Queue<UnityTransportEndpoint> _accepted =
            new Queue<UnityTransportEndpoint>();
        private readonly Queue<ConnectionId> _disconnected;
        private readonly NetworkBufferPool _pool;
        private readonly UnityTransportSettings _settings;
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

        internal UnityTransportDriver(in UnityTransportSettings settings, bool listener)
        {
            _settings = settings;
            _listener = listener;
            _disconnected = new Queue<ConnectionId>(_settings.MaximumConnections);
            _pool = new NetworkBufferPool(listener
                ? NetworkBufferPool.DefaultServerRetainedBytes
                : NetworkBufferPool.DefaultClientRetainedBytes);
            var networkSettings = new NetworkSettings();
            try
            {
                networkSettings.WithNetworkConfigParameters(maxMessageSize: NetworkMessageSize);
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

            var removed = new List<NetworkConnection>();
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
                        removed.Add(pair.Key);
                        break;
                    }
                    if (type != NetworkEvent.Type.Data)
                        continue;
                    if (reader.Length <= 0 || reader.Length > UnityTransportSettings.MaximumReliableBytes)
                    {
                        _dropped++;
                        _malformedPackets++;
                        continue;
                    }
                    if (endpoint.QueuedPackets >= _settings.ReceiveQueueCapacity)
                    {
                        _dropped++;
                        _receiveQueueOverflows++;
                        continue;
                    }
                    var bytes = new byte[reader.Length];
                    for (var index = 0; index < bytes.Length; index++)
                        bytes[index] = reader.ReadByte();
                    var packet = _pool.Copy(bytes);
                    if (!NetworkPacket.TryDecode(packet, out var header, out _))
                    {
                        packet.Dispose();
                        _dropped++;
                        _malformedPackets++;
                        continue;
                    }
                    var reliable = header.Flags == PacketFlags.ReliableOrdered;
                    var expectedPipeline = reliable ? _reliable : _unreliable;
                    var limit = reliable
                        ? UnityTransportSettings.MaximumReliableBytes
                        : _settings.MaximumUnreliableBytes;
                    if (!pipeline.Equals(expectedPipeline) || packet.Length > limit)
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

            for (var index = 0; index < removed.Count; index++)
            {
                var id = removed[index];
                if (_connections.Remove(id, out var endpoint))
                    endpoint.DisposeFromDriver();
            }
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
                    _dropped++;
                    _sendFailures++;
                    return false;
                }
                if (!NetworkPacket.TryDecode(packet, out header, out _))
                {
                    _dropped++;
                    _sendFailures++;
                    return false;
                }
                var reliable = header.Flags == PacketFlags.ReliableOrdered;
                if (packet.Length > (reliable
                        ? UnityTransportSettings.MaximumReliableBytes
                        : _settings.MaximumUnreliableBytes))
                {
                    _dropped++;
                    _sendFailures++;
                    return false;
                }
                var pipeline = reliable ? _reliable : _unreliable;
                if (_driver.BeginSend(pipeline, endpoint.NativeConnection, out var writer, packet.Length) != 0)
                {
                    _dropped++;
                    _sendFailures++;
                    return false;
                }
                var span = packet.Span;
                for (var index = 0; index < span.Length; index++)
                    writer.WriteByte(span[index]);
                if (_driver.EndSend(writer) < 0)
                {
                    _dropped++;
                    _sendFailures++;
                    return false;
                }
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
                return true;
            }
            finally
            {
                packet.Dispose();
            }
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

        private UnityTransportEndpoint Add(NetworkConnection connection, bool accepted)
        {
            var id = checked(++_nextConnection);
            var endpoint = new UnityTransportEndpoint(this, connection, new ConnectionId(id), _settings.ReceiveQueueCapacity);
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
        private bool _disposed;

        internal UnityTransportEndpoint(UnityTransportDriver owner, NetworkConnection connection,
            ConnectionId id, int queueCapacity)
        {
            _owner = owner;
            NativeConnection = connection;
            Connection = id;
            _incoming = new Queue<NetworkBufferLease>(queueCapacity);
        }

        public ConnectionId Connection { get; }
        internal NetworkConnection NativeConnection { get; }
        internal bool IsConnected { get; set; }
        internal bool IsDisposed => _disposed;
        internal int QueuedPackets => _incoming.Count;

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

        internal void DisposeFromDriver()
        {
            if (_disposed)
                return;
            _disposed = true;
            IsConnected = false;
            while (_incoming.Count > 0)
                _incoming.Dequeue().Dispose();
        }
    }
}
