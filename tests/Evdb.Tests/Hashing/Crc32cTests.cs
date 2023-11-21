using Evdb.Hashing;
using System.Text;

namespace Evdb.Tests.Hashing;

public class Crc32cTests
{
    [Test]
    public void Compute__Same_Data__Same_Hash()
    {
        // Arrange
        byte[] data = new byte[1024];

        Random.Shared.NextBytes(data);

        // Act
        uint v1 = Crc32c.Compute(data).Value;
        uint v2 = Crc32c.Compute(data).Value;

        // Assert
        Assert.That(v1, Is.EqualTo(v2));
    }

    [Test]
    public void Compute__Zeros__Retuns_Expected_Hash()
    {
        // Arrange
        byte[] data = new byte[32];

        // Act
        uint v1 = Crc32c.Compute(data).Value;

        // Assert
        Assert.That(v1, Is.EqualTo(0x8A9136AA));
    }

    [Test]
    public void Compute__Ones__Retuns_Expected_Hash()
    {
        // Arrange
        byte[] data = new byte[32];

        data.AsSpan().Fill(0xFF);

        // Act
        uint v1 = Crc32c.Compute(data).Value;

        // Assert
        Assert.That(v1, Is.EqualTo(0x62A8AB43));
    }

    [TestCase("123456789", 0xE3069283)]
    public void Compute__Returns_Expected_Hash(string str, uint value)
    {
        // Arrange
        byte[] data = Encoding.ASCII.GetBytes(str);

        // Act
        uint v1 = Crc32c.Compute(data).Value;

        // Assert
        Assert.That(v1, Is.EqualTo(value));
    }
}
