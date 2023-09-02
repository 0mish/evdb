using Evdb.Indexing;
using Evdb.IO;

namespace Evdb.Tests.Indexes.Common;

public class LogWriterTests
{
    [Test]
    public void Ctor__ArgumentNull()
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => new LogWriter(null!, ""), Throws.ArgumentNullException);
            Assert.That(() => new LogWriter(new FileSystem(), null!), Throws.ArgumentNullException);
        });
    }
}
