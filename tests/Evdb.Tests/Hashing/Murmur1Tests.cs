using Evdb.Hashing;

namespace Evdb.Tests.Hashing;

public class Murmur1Tests
{
    [Test]
    public void Compute__SameData__SameHash()
    {
        byte[] data = new byte[1024];

        Random.Shared.NextBytes(data);

        uint v1 = Murmur1.Compute(data).Value;
        uint v2 = Murmur1.Compute(data).Value;

        Assert.That(v1, Is.EqualTo(v2));
    }
}
