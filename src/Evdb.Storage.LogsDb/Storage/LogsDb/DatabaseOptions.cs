using Evdb.IO;

namespace Evdb.Storage.LogsDb;

public sealed class DatabaseOptions
{
    public ulong WriteLogSize { get; set; } = 1024 * 4;
    public ulong ManifestLogSize { get; set; } = 1024 * 4;
    public ulong DataBlockSize { get; set; } = 1024 * 4;
    public ulong BloomBlockSize { get; set; } = 1024 * 4;
    public int VirtualTableSize { get; set; } = 1024 * 16;

    public string Path { get; set; } = default!;
    public IFileSystem FileSystem { get; set; } = new FileSystem();
}
