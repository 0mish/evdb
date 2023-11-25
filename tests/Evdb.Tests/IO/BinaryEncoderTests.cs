using Evdb.IO;

namespace Evdb.Tests.IO;

internal class BinaryEncoderTests
{
    [TestCase(0x0u, 4u, new byte[] { 0x00, 0x00, 0x00, 0x00 })]
    public void UInt32__BytesWritten(uint value, ulong expectedPosition, byte[] expectedBuffer)
    {
        // Arrange
        BinaryEncoder encoder = new();

        // Act
        encoder.UInt32(value);

        // Assert
        Assert.Multiple(() =>
        {
            Assert.That(encoder.Length, Is.EqualTo(expectedPosition));
            Assert.That(encoder.Span.ToArray(), Is.EqualTo(expectedBuffer));
        });
    }

    [TestCase(0x20u, 1u, new byte[] { 0x20 })]
    [TestCase(0x7Fu, 1u, new byte[] { 0x7F })]
    [TestCase(0xFFu, 2u, new byte[] { 0xFF, 0x01 })]
    [TestCase(uint.MinValue, 1u, new byte[] { 0x00 })]
    [TestCase(uint.MaxValue, 5u, new byte[] { 0xFF, 0xFF, 0xFF, 0xFF, 0x0F })]
    public void VarUInt32__BytesWritten(uint value, ulong expectedPosition, byte[] expectedBuffer)
    {
        // Arrange
        BinaryEncoder encoder = new();

        // Act
        encoder.VarUInt32(value);

        // Assert
        Assert.Multiple(() =>
        {
            Assert.That(encoder.Length, Is.EqualTo(expectedPosition));
            Assert.That(encoder.Span.ToArray(), Is.EqualTo(expectedBuffer));
        });
    }

    [TestCase(0x20u, 1u, new byte[] { 0x20 })]
    [TestCase(0x7Fu, 1u, new byte[] { 0x7F })]
    [TestCase(0xFFu, 2u, new byte[] { 0xFF, 0x01 })]
    [TestCase(ulong.MinValue, 1u, new byte[] { 0x00 })]
    [TestCase(ulong.MaxValue, 10u, new byte[] { 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01 })]
    public void VarUInt64__BytesWritten(ulong value, ulong expectedPosition, byte[] expectedBuffer)
    {
        // Arrange
        BinaryEncoder encoder = new();

        // Act
        encoder.VarUInt64(value);

        // Assert
        Assert.Multiple(() =>
        {
            Assert.That(encoder.Length, Is.EqualTo(expectedPosition));
            Assert.That(encoder.Span.ToArray(), Is.EqualTo(expectedBuffer));
        });
    }

    [TestCase(new byte[] { }, 1u, new byte[] { 0x00 })]
    [TestCase(new byte[] { 0x20 }, 2u, new byte[] { 0x01, 0x20 })]
    public void ByteArray__BytesWritten(byte[] value, ulong expectedPosition, byte[] expectedBuffer)
    {
        // Arrange
        BinaryEncoder encoder = new();

        // Act
        encoder.ByteArray(value);

        // Assert
        Assert.Multiple(() =>
        {
            Assert.That(encoder.Length, Is.EqualTo(expectedPosition));
            Assert.That(encoder.Span.ToArray(), Is.EqualTo(expectedBuffer));
        });
    }
}
