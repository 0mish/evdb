using LogsDb.IO;

namespace LogsDb.Tests.IO;

internal class BinaryDecoderTests
{
    [TestCase(new byte[] { 0x20 }, true, 1u, 0x20u)]
    [TestCase(new byte[] { 0x7F }, true, 1u, 0x7Fu)]
    [TestCase(new byte[] { 0xFF }, false, 1u, 0x00u)]
    [TestCase(new byte[] { 0xFF, 0x01 }, true, 2u, 0xFFu)]
    [TestCase(new byte[] { 0x00 }, true, 1u, uint.MinValue)]
    [TestCase(new byte[] { 0xFF, 0xFF, 0xFF, 0xFF, 0x0F }, true, 5u, uint.MaxValue)]
    public void VarUInt32__BytesRead(byte[] buffer, bool expectedResult, ulong expectedPosition, uint expectedValue)
    {
        // Arrange
        BinaryDecoder encoder = new(buffer);

        // Act
        bool result = encoder.VarUInt32(out uint value);

        // Assert
        Assert.That(result, Is.EqualTo(expectedResult));
        Assert.That(encoder.Position, Is.EqualTo(expectedPosition));

        if (result)
        {
            Assert.That(value, Is.EqualTo(expectedValue));
        }
    }

    [TestCase(new byte[] { 0x20 }, true, 1u, 0x20u)]
    [TestCase(new byte[] { 0x7F }, true, 1u, 0x7Fu)]
    [TestCase(new byte[] { 0xFF }, false, 1u, 0x00u)]
    [TestCase(new byte[] { 0xFF, 0x01 }, true, 2u, 0xFFu)]
    [TestCase(new byte[] { 0x00 }, true, 1u, uint.MinValue)]
    [TestCase(new byte[] { 0xFF, 0xFF, 0xFF, 0xFF, 0x0F }, true, 5u, uint.MaxValue)]
    [TestCase(new byte[] { 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01 }, true, 10u, ulong.MaxValue)]
    public void VarUInt64__BytesRead(byte[] buffer, bool expectedResult, ulong expectedPosition, ulong expectedValue)
    {
        // Arrange
        BinaryDecoder encoder = new(buffer);

        // Act
        bool result = encoder.VarUInt64(out ulong value);

        // Assert
        Assert.That(result, Is.EqualTo(expectedResult));
        Assert.That(encoder.Position, Is.EqualTo(expectedPosition));

        if (result)
        {
            Assert.That(value, Is.EqualTo(expectedValue));
        }
    }
}
