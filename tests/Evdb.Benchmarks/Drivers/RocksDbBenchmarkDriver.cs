using Evdb.Benchmarks.IO;
using RocksDbSharp;

namespace Evdb.Benchmarks.Drivers;

public struct RocksDbBenchmarkDriverOptions
{
    public bool AutoCompaction { get; set; }
    public ulong WriteBufferSize { get; set; }
    public bool ParanoidChecks { get; set; }
}

public readonly struct RocksDbBenchmarkDriver : IBenchmarkDriver, IDisposable
{
    private readonly RocksDb _database;
    private readonly DbOptions _options;
    private readonly WriteOptions _woptions;

    public RocksDbBenchmarkDriver(RocksDbBenchmarkDriverOptions options)
    {
        if (Directory.Exists("rocksdb-db"))
        {
            Directory.Delete("rocksdb-db", recursive: true);
        }

        _options = new DbOptions()
            .SetCreateIfMissing(true)
            .SetWriteBufferSize(options.WriteBufferSize)
            .SetDisableAutoCompactions(options.AutoCompaction ? 0 : 1)
            .SetParanoidChecks(options.ParanoidChecks);

        _woptions = new WriteOptions()
            .SetSync(false);

        _database = RocksDb.Open(_options, "rocksdb-db");
    }

    public readonly bool TryGet(ReadOnlySpan<byte> key, out ReadOnlySpan<byte> value)
    {
        try
        {
            value = _database.Get(key);

            return true;
        }
        catch
        {
            value = default;

            return false;
        }
    }

    public readonly bool TrySet(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
    {
        try
        {
            _database.Put(key, value, writeOptions: _woptions);

            return true;
        }
        catch
        {
            return false;
        }
    }

    public readonly void WaitCompaction()
    {
        // RocksDbSharp does not support rocksdb_wait_for_compact, so we use a heuristic instead.
        DirectoryUtils.WaitNoModification(path: "rocksdb-db");
    }

    public readonly void Dispose()
    {
        _database.Dispose();
    }
}
