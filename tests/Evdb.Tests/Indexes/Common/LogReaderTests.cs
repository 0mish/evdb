using Evdb.Indexes.Common;
using Evdb.IO;

namespace Evdb.Tests.Indexes.Common;

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
