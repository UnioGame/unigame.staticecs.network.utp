namespace UniGame.StaticEcs.Network.UnityTransport
{
    using System;

    /// <summary>Defines immutable packet limits supported by the Unity Transport adapter.</summary>
    public static class UnityTransportLimits
    {
        /// <summary>Maximum complete unreliable packet bytes, including <see cref="PacketHeader"/>.</summary>
        public const int MaximumUnreliableBytes = 1400;

        // Unity.Networking.Transport.Pipelines.ReliableUtility.PacketHeader wire size: Type(1) +
        // AckMaskLength(1) + ProcessingTime(2) + SequenceId(2) + AckedSequenceId(2) = 8 bytes.
        // Verified in com.unity.transport 6.6.0, Runtime/Pipelines/ReliableUtility.cs.
        private const int ReliableBaseHeaderBytes = 8;
        // Unity.Networking.Transport.FragmentationPipelineStage.FragHeaderCapacity in a non-debug
        // build (2 bits First/Last flags + 14 bits sequence number, packed into a short).
        // Verified in com.unity.transport 6.6.0, Runtime/Pipelines/FragmentationPipelineStage.cs.
        private const int FragmentationHeaderBytes = 2;

        /// <summary>
        /// Computes the reliable pipeline ack-mask wire bytes for a given window size, mirroring
        /// <c>Unity.Networking.Transport.Pipelines.ReliableUtility.MaxPacketHeaderWireSize</c>: windows of
        /// 32 or less use a 4-byte mask, windows of 64 or less use a fixed 8-byte mask, and windows
        /// above 64 use one bit per packet (<paramref name="windowSize"/> / 8 bytes; UTP requires such
        /// windows to be a multiple of 8).
        /// </summary>
        public static int ComputeReliableAckMaskBytes(int windowSize) =>
            windowSize <= 32 ? sizeof(uint) : windowSize <= 64 ? sizeof(ulong) : windowSize / 8;

        /// <summary>
        /// Computes the wire header bytes Unity Transport adds around one fragmented reliable
        /// packet for <paramref name="windowSize"/>: the fragmentation stage's 2-byte header plus
        /// the reliable stage's base header and ack mask. This must stay outside the public
        /// <see cref="UnityTransportSettings.MaximumReliableBytes"/> budget passed to
        /// <c>WithFragmentationStageParameters</c>, or the largest complete reliable packet would
        /// overflow the fragmentation payload capacity.
        /// </summary>
        public static int ComputeReliableFragmentationPipelineHeaderBytes(int windowSize) =>
            FragmentationHeaderBytes + ReliableBaseHeaderBytes + ComputeReliableAckMaskBytes(windowSize);
    }

    /// <summary>Configures one Unity Transport driver and its bounded packet queues.</summary>
    [Serializable]
    public struct UnityTransportSettings
    {
        /// <summary>Default game port.</summary>
        public const ushort DefaultPort = 7777;
        /// <summary>Maximum complete reliable packet bytes, including <see cref="PacketHeader"/>.</summary>
        public const int MaximumReliableBytes = 64 * 1024;
        /// <summary>Adapter default reliable window size; UTP's own native default is 32.</summary>
        public const int DefaultReliableWindowSize = 64;
        /// <summary>
        /// Largest reliable window size UTP allows: <c>byte.MaxValue * ReliableAckMask.AcksPerByte</c>
        /// (255 * 8), verified as <c>ReliableUtility.MaxWindowSize</c> in com.unity.transport 6.6.0,
        /// Runtime/Pipelines/ReliableUtility.cs. Values above 64 must be a multiple of 8.
        /// </summary>
        public const int MaximumReliableWindowSize = 2040;
        /// <summary>
        /// Floor applied to a derived <see cref="ServerSendQueueCapacity"/>, matching UTP's own native
        /// default (<c>NetworkParameterConstants.SendQueueCapacity</c> / <c>ReceiveQueueCapacity</c>,
        /// both 512 in com.unity.transport 6.6.0, Runtime/NetworkParams.cs), so a small
        /// <see cref="MaximumConnections"/> never starves the shared native queue below the built-in
        /// baseline.
        /// </summary>
        public const int MinimumServerSendQueueCapacity = 512;
        /// <summary>Default per-connection managed reliable byte budget; four maximum reliable packets.</summary>
        public const long DefaultReliableSendBytesCapacity = MaximumReliableBytes * 4L;

        /// <summary>Address used by a client or listener.</summary>
        public string Address;
        /// <summary>UDP port.</summary>
        public ushort Port;
        /// <summary>Maximum complete unreliable packet bytes, including <see cref="PacketHeader"/>.</summary>
        public int MaximumUnreliableBytes;
        /// <summary>Maximum queued received packets per connection.</summary>
        public int ReceiveQueueCapacity;
        /// <summary>Maximum accepted server connections.</summary>
        public int MaximumConnections;
        /// <summary>
        /// Reliable pipeline in-flight window size in packets; zero selects
        /// <see cref="DefaultReliableWindowSize"/>. Values above 64 are rounded down to a multiple of
        /// 8 (UTP's own requirement) and the whole value is capped at <see cref="MaximumReliableWindowSize"/>.
        /// </summary>
        public int ReliableWindowSize;
        /// <summary>
        /// Native UTP driver send and receive queue capacity in packets for a listener, shared across
        /// all its connections; zero derives it from <see cref="MaximumConnections"/> and
        /// <see cref="ReliableWindowSize"/> via <see cref="ComputeServerSendQueueCapacity"/>. Unused by
        /// a client host.
        /// </summary>
        public int ServerSendQueueCapacity;
        /// <summary>
        /// Maximum reliable bytes this adapter holds per connection in its managed FIFO once UTP's
        /// native send queue rejects a send; zero selects <see cref="DefaultReliableSendBytesCapacity"/>.
        /// This bounds pending bytes in addition to the packet-count FIFO limit, so one connection
        /// cannot back up an unbounded amount of memory with few but very large reliable packets.
        /// </summary>
        public long ReliableSendBytesCapacity;

        /// <summary>Gets conservative defaults for a separated endpoint.</summary>
        public static UnityTransportSettings Default => new UnityTransportSettings
        {
            Address = "127.0.0.1",
            Port = DefaultPort,
            MaximumUnreliableBytes = UnityTransportLimits.MaximumUnreliableBytes,
            ReceiveQueueCapacity = 256,
            MaximumConnections = 128,
            ReliableWindowSize = DefaultReliableWindowSize,
        };

        /// <summary>
        /// Derives a listener's native send/receive queue capacity from its connection bound and
        /// reliable window, floored at <see cref="MinimumServerSendQueueCapacity"/>. One reliable
        /// window per connection is the amount of in-flight packets UTP itself can hold before a
        /// peer's channel backs up; this reserves <b>two</b> such windows per configured connection
        /// so a fully saturated server keeps headroom against transient bursts (a resend wave,
        /// coalesced sends after a stall) instead of sitting exactly at the edge of
        /// <c>Error.StatusCode.NetworkSendQueueFull</c>.
        /// </summary>
        public static int ComputeServerSendQueueCapacity(int maximumConnections, int reliableWindowSize)
        {
            var connections = maximumConnections <= 0 ? 1 : maximumConnections;
            var window = reliableWindowSize <= 0 ? DefaultReliableWindowSize : reliableWindowSize;
            var derived = 2L * connections * window;
            if (derived > int.MaxValue)
                derived = int.MaxValue;
            return (int)Math.Max(MinimumServerSendQueueCapacity, derived);
        }

        /// <summary>Validates and normalizes optional zero values.</summary>
        public UnityTransportSettings Normalize(bool listener)
        {
            var value = this;
            if (string.IsNullOrWhiteSpace(value.Address))
                value.Address = listener ? "0.0.0.0" : "127.0.0.1";
            if (value.Port == 0)
                value.Port = DefaultPort;
            if (value.MaximumUnreliableBytes <= PacketHeader.Size ||
                value.MaximumUnreliableBytes > UnityTransportLimits.MaximumUnreliableBytes)
                value.MaximumUnreliableBytes = UnityTransportLimits.MaximumUnreliableBytes;
            if (value.ReceiveQueueCapacity <= 0)
                value.ReceiveQueueCapacity = 256;
            if (value.MaximumConnections <= 0)
                value.MaximumConnections = 128;
            if (value.ReliableWindowSize <= 0)
                value.ReliableWindowSize = DefaultReliableWindowSize;
            else if (value.ReliableWindowSize > MaximumReliableWindowSize)
                value.ReliableWindowSize = MaximumReliableWindowSize;
            if (value.ReliableWindowSize > 64)
            {
                var remainder = value.ReliableWindowSize % 8;
                if (remainder != 0)
                    value.ReliableWindowSize -= remainder;
            }
            if (value.ServerSendQueueCapacity <= 0)
                value.ServerSendQueueCapacity = ComputeServerSendQueueCapacity(
                    value.MaximumConnections, value.ReliableWindowSize);
            if (value.ReliableSendBytesCapacity <= 0)
                value.ReliableSendBytesCapacity = DefaultReliableSendBytesCapacity;
            return value;
        }
    }

    /// <summary>Captures transport counters without exposing driver-native state.</summary>
    public struct UnityTransportDiagnostics
    {
        /// <summary>Number of active endpoint objects.</summary>
        public int Connections;
        /// <summary>Number of packets accepted from the driver.</summary>
        public long ReceivedPackets;
        /// <summary>Number of reliable packets accepted from the driver.</summary>
        public long ReliableReceivedPackets;
        /// <summary>Number of reliable bytes accepted from the driver.</summary>
        public long ReliableReceivedBytes;
        /// <summary>Number of unreliable packets accepted from the driver.</summary>
        public long UnreliableReceivedPackets;
        /// <summary>Number of unreliable bytes accepted from the driver.</summary>
        public long UnreliableReceivedBytes;
        /// <summary>Number of packets submitted to the driver.</summary>
        public long SentPackets;
        /// <summary>Number of reliable packets submitted to the driver.</summary>
        public long ReliableSentPackets;
        /// <summary>Number of reliable bytes submitted to the driver.</summary>
        public long ReliableSentBytes;
        /// <summary>Number of unreliable packets submitted to the driver.</summary>
        public long UnreliableSentPackets;
        /// <summary>Number of unreliable bytes submitted to the driver.</summary>
        public long UnreliableSentBytes;
        /// <summary>Number of packets or lifecycle notifications dropped by limits, queues, or UTP.</summary>
        public long DroppedPackets;
        /// <summary>Number of received packets rejected because the bounded receive queue was full.</summary>
        public long ReceiveQueueOverflows;
        /// <summary>Number of packets rejected because they were malformed or used an invalid pipeline.</summary>
        public long MalformedPackets;
        /// <summary>Number of packets rejected while attempting to send.</summary>
        public long SendFailures;
        /// <summary>Number of observed transport disconnects.</summary>
        public long Disconnects;
        /// <summary>Number of reliable packets currently awaiting a later driver update.</summary>
        public int PendingReliablePackets;
        /// <summary>Number of reliable bytes currently awaiting a later driver update.</summary>
        public long PendingReliableBytes;
        /// <summary>Highest observed number of pending reliable packets.</summary>
        public int PendingReliablePacketsHighWater;
        /// <summary>Highest observed number of pending reliable bytes.</summary>
        public long PendingReliableBytesHighWater;
        /// <summary>Number of reliable packets rejected because the fixed send queue was full.</summary>
        public long ReliableSendQueueOverflows;
        /// <summary>Number of currently queued receive packets.</summary>
        public int QueuedPackets;
        /// <summary>Number of receive leases currently owned outside the transport pool.</summary>
        public int OutstandingLeases;

        // The fields below surface Unity.Networking.Transport.NetworkDriver.GetStatistics and
        // NetworkDriver.GetConnectionStatistics (com.unity.transport 6.6.0,
        // Runtime/Analytics/DriverStatistics.cs and ConnectionStatistics.cs), which are always
        // available: the driver always installs its internal AnalyticsLayer.

        /// <summary>Cumulative native payload bytes received by the driver over its lifetime.</summary>
        public ulong NativeReceivedTotalBytes;
        /// <summary>Cumulative native payload bytes transmitted by the driver over its lifetime.</summary>
        public ulong NativeSentTotalBytes;
        /// <summary>Cumulative native packets received by the driver over its lifetime.</summary>
        public ulong NativeReceivedTotalPackets;
        /// <summary>Cumulative native packets transmitted by the driver over its lifetime.</summary>
        public ulong NativeSentTotalPackets;
        /// <summary>Average native receive queue usage in packets over the driver's lifetime.</summary>
        public float NativeReceiveQueueMeanUsage;
        /// <summary>Average native send queue usage in packets over the driver's lifetime.</summary>
        public float NativeSendQueueMeanUsage;
        /// <summary>Highest observed native receive queue usage in packets.</summary>
        public uint NativeReceiveQueueMaximumUsage;
        /// <summary>Highest observed native send queue usage in packets.</summary>
        public uint NativeSendQueueMaximumUsage;
        /// <summary>Sum, across currently active connections, of native reliable packets resent.</summary>
        public long NativeReliableResentPackets;
        /// <summary>Sum, across currently active connections, of native reliable packets UTP dropped.</summary>
        public long NativeReliableDroppedPackets;
        /// <summary>Sum, across currently active connections, of native reliable duplicate packets.</summary>
        public long NativeReliableDuplicatedPackets;
        /// <summary>Sum, across currently active connections, of native reliable out-of-order packets.</summary>
        public long NativeReliableOutOfOrderPackets;
        /// <summary>
        /// Average of <c>ConnectionStatistics.Latency.Mean</c> across currently active connections
        /// that have taken at least one latency sample; zero when none have.
        /// </summary>
        public float NativeMeanLatencyMs;
        /// <summary>Highest <c>ConnectionStatistics.Latency.Maximum</c> observed across active connections.</summary>
        public uint NativeMaximumLatencyMs;
    }
}
