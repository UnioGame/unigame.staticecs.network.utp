namespace UniGame.StaticEcs.Network.UnityTransport.Tests
{
    using NUnit.Framework;

    internal sealed class UnityTransportSettingsTests
    {
        /// <summary>Verifies optional values normalize to bounded client defaults.</summary>
        [Test]
        public void NormalizeAppliesBoundedDefaults()
        {
            var value = default(UnityTransportSettings).Normalize(false);

            Assert.AreEqual("127.0.0.1", value.Address);
            Assert.AreEqual(UnityTransportSettings.DefaultPort, value.Port);
            Assert.AreEqual(UnityTransportLimits.MaximumUnreliableBytes,
                value.MaximumUnreliableBytes);
            Assert.AreEqual(256, value.ReceiveQueueCapacity);
            Assert.AreEqual(128, value.MaximumConnections);
            Assert.AreEqual(UnityTransportSettings.DefaultReliableWindowSize,
                value.ReliableWindowSize);
            Assert.AreEqual(UnityTransportSettings.DefaultReliableSendBytesCapacity,
                value.ReliableSendBytesCapacity);
        }

        /// <summary>Verifies an oversized reliable window is capped at UTP's own maximum.</summary>
        [Test]
        public void NormalizeCapsOversizedReliableWindowSize()
        {
            var value = UnityTransportSettings.Default;
            value.ReliableWindowSize = UnityTransportSettings.MaximumReliableWindowSize + 800;

            Assert.AreEqual(UnityTransportSettings.MaximumReliableWindowSize,
                value.Normalize(false).ReliableWindowSize);
        }

        /// <summary>Verifies a zero or negative reliable window falls back to the adapter default.</summary>
        [TestCase(0)]
        [TestCase(-32)]
        public void NormalizeAppliesDefaultForNonPositiveReliableWindowSize(int requested)
        {
            var value = UnityTransportSettings.Default;
            value.ReliableWindowSize = requested;

            Assert.AreEqual(UnityTransportSettings.DefaultReliableWindowSize,
                value.Normalize(false).ReliableWindowSize);
        }

        /// <summary>
        /// Verifies a reliable window above 64 is rounded down to a multiple of 8, matching UTP's
        /// own requirement (Unity.Networking.Transport.Pipelines.ReliableUtility.Parameters.WindowSize).
        /// </summary>
        [TestCase(65, 64)]
        [TestCase(71, 64)]
        [TestCase(100, 96)]
        [TestCase(128, 128)]
        public void NormalizeRoundsReliableWindowSizeAboveSixtyFourDownToMultipleOfEight(
            int requested, int expected)
        {
            var value = UnityTransportSettings.Default;
            value.ReliableWindowSize = requested;

            Assert.AreEqual(expected, value.Normalize(false).ReliableWindowSize);
        }

        /// <summary>Verifies a reliable window at or below 64 needs no alignment.</summary>
        [TestCase(1)]
        [TestCase(32)]
        [TestCase(50)]
        [TestCase(64)]
        public void NormalizePreservesReliableWindowSizeAtOrBelowSixtyFour(int requested)
        {
            var value = UnityTransportSettings.Default;
            value.ReliableWindowSize = requested;

            Assert.AreEqual(requested, value.Normalize(false).ReliableWindowSize);
        }

        /// <summary>
        /// Verifies the listener native queue capacity derives from connections and window size,
        /// reserving two windows per connection so a saturated server keeps headroom against
        /// transient bursts instead of sitting exactly at NetworkSendQueueFull.
        /// </summary>
        [Test]
        public void NormalizeDerivesServerSendQueueCapacityFromConnectionsAndWindow()
        {
            var value = UnityTransportSettings.Default;
            value.MaximumConnections = 200;
            value.ReliableWindowSize = 32;

            Assert.AreEqual(2 * 200 * 32, value.Normalize(true).ServerSendQueueCapacity);
        }

        /// <summary>Verifies a small connection bound still floors the derived native queue capacity.</summary>
        [Test]
        public void NormalizeFloorsServerSendQueueCapacityForSmallConnectionBounds()
        {
            var value = UnityTransportSettings.Default;
            value.MaximumConnections = 4;
            value.ReliableWindowSize = 8;

            Assert.AreEqual(UnityTransportSettings.MinimumServerSendQueueCapacity,
                value.Normalize(true).ServerSendQueueCapacity);
        }

        /// <summary>Verifies an explicit override bypasses derivation entirely.</summary>
        [Test]
        public void NormalizePreservesExplicitServerSendQueueCapacityOverride()
        {
            var value = UnityTransportSettings.Default;
            value.ServerSendQueueCapacity = 77;

            Assert.AreEqual(77, value.Normalize(true).ServerSendQueueCapacity);
        }

        /// <summary>Verifies the static helper saturates rather than overflowing for extreme inputs.</summary>
        [Test]
        public void ComputeServerSendQueueCapacitySaturatesAtIntMax()
        {
            Assert.AreEqual(int.MaxValue,
                UnityTransportSettings.ComputeServerSendQueueCapacity(int.MaxValue,
                    UnityTransportSettings.MaximumReliableWindowSize));
        }

        /// <summary>Verifies the static helper falls back to defaults for non-positive inputs.</summary>
        [Test]
        public void ComputeServerSendQueueCapacityUsesDefaultsForNonPositiveInputs()
        {
            Assert.AreEqual(UnityTransportSettings.MinimumServerSendQueueCapacity,
                UnityTransportSettings.ComputeServerSendQueueCapacity(0, 0));
        }

        /// <summary>
        /// Verifies the fragmentation header math matches Unity Transport's own reliable header
        /// sizing (Runtime/Pipelines/ReliableUtility.cs MaxPacketHeaderWireSize plus the
        /// FragmentationPipelineStage's 2-byte header): windows of 32 or less use a 4-byte ack
        /// mask, windows of 64 or less use a fixed 8-byte mask, and windows above 64 use one bit
        /// per packet.
        /// </summary>
        [TestCase(16, 14)]
        [TestCase(32, 14)]
        [TestCase(33, 18)]
        [TestCase(64, 18)]
        [TestCase(128, 26)]
        [TestCase(2040, 265)]
        public void ComputeReliableFragmentationPipelineHeaderBytesMatchesUtpFormula(
            int windowSize, int expected)
        {
            Assert.AreEqual(expected,
                UnityTransportLimits.ComputeReliableFragmentationPipelineHeaderBytes(windowSize));
        }

        /// <summary>Verifies oversized unreliable settings normalize to the supported UTP limit.</summary>
        [Test]
        public void NormalizeCapsOversizedUnreliablePackets()
        {
            var value = UnityTransportSettings.Default;
            value.MaximumUnreliableBytes = UnityTransportLimits.MaximumUnreliableBytes + 1;

            Assert.AreEqual(UnityTransportLimits.MaximumUnreliableBytes,
                value.Normalize(false).MaximumUnreliableBytes);
        }

        /// <summary>Verifies the smallest complete unreliable packet remains an explicit capability.</summary>
        [Test]
        public void NormalizePreservesMinimumCompleteUnreliablePacket()
        {
            var value = UnityTransportSettings.Default;
            value.MaximumUnreliableBytes = PacketHeader.Size + 1;

            Assert.AreEqual(PacketHeader.Size + 1,
                value.Normalize(false).MaximumUnreliableBytes);
        }

        /// <summary>Verifies rejected sends still consume the caller-owned lease.</summary>
        [Test]
        public void RejectedPacketLeaseIsConsumed()
        {
            using var pool = new NetworkBufferPool(1024);
            using var host = new UnityTransportClientHost(UnityTransportSettings.Default);
            var packet = pool.Copy(new byte[1]);
            Assert.AreEqual(UnityTransportSettings.MaximumReliableBytes,
                host.Endpoint.MaxReliablePayloadBytes);
            Assert.AreEqual(UnityTransportSettings.Default.MaximumUnreliableBytes,
                host.Endpoint.MaxUnreliablePayloadBytes);

            Assert.IsFalse(host.Endpoint.TrySend(packet));
            Assert.AreEqual(0, packet.Length);
            Assert.AreEqual(0, pool.CaptureDiagnostics().OutstandingLeases);
            Assert.That(host.CaptureDiagnostics().SendFailures, Is.GreaterThanOrEqualTo(1));
        }
    }
}
