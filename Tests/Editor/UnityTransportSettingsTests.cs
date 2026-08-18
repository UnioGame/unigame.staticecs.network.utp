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

        /// <summary>Verifies rejected sends still consume the caller-owned lease.</summary>
        [Test]
        public void RejectedPacketLeaseIsConsumed()
        {
            using var pool = new NetworkBufferPool(1024);
            using var host = new UnityTransportClientHost(UnityTransportSettings.Default);
            var packet = pool.Copy(new byte[1]);

            Assert.IsFalse(host.Endpoint.TrySend(packet));
            Assert.AreEqual(0, packet.Length);
            Assert.AreEqual(0, pool.CaptureDiagnostics().OutstandingLeases);
            Assert.That(host.CaptureDiagnostics().SendFailures, Is.GreaterThanOrEqualTo(1));
        }
    }
}
