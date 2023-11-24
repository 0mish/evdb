namespace Evdb.Storage.RocksDb;

public class DatabaseOptions
{
    public string Path { get; set; } = default!;
    public bool AutoCompaction { get; set; }
    public ulong WriteBufferSize { get; set; }
    public bool ParanoidChecks { get; set; }
}
