using Evdb.IO;

namespace Evdb.Indexing;

public sealed class DatabaseOptions
{
    public ulong DataBlockSize { get; set; } = 1024 * 4;
    public ulong BloomBlockSize { get; set; } = 1024 * 4;
    public int VirtualTableSize { get; set; } = 1024 * 16;

    public string Path { get; set; } = default!;
    public IFileSystem FileSystem { get; set; } = new FileSystem();
}
