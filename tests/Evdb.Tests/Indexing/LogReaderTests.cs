using Evdb.Indexing;
using Evdb.IO;

namespace Evdb.Tests.Indexing;

public class LogReaderTests
{
    [Test]
    public void Ctor__ArgumentNull()
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => new LogReader(null!, ""), Throws.ArgumentNullException);
            Assert.That(() => new LogReader(new FileSystem(), null!), Throws.ArgumentNullException);
        });
    }
}
