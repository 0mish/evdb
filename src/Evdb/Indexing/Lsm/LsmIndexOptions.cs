using Evdb.IO;

namespace Evdb.Indexing.Lsm;

internal sealed class LsmIndexOptions
{
    public int VirtualTableSize { get; set; } = 1024 * 16;

    public string Path { get; set; } = default!;
    public IFileSystem FileSystem { get; set; } = new FileSystem();
}
