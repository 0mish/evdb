using Evdb.Indexing;

namespace Evdb.Benchmarks.Drivers;

public struct EvdbBenchmarkDriverOptions
{
    public int VirtualTableSize { get; set; }
}

public readonly struct EvdbBenchmarkDriver : IBenchmarkDriver, IDisposable
{
    private readonly Database _database;
    private readonly DatabaseOptions _options;

    public EvdbBenchmarkDriver(EvdbBenchmarkDriverOptions options)
    {
        if (Directory.Exists("evdb-db"))
        {
            Directory.Delete("evdb-db", recursive: true);
        }

        _options = new DatabaseOptions
        {
            Path = "evdb-db",
            VirtualTableSize = options.VirtualTableSize
        };

        _database = new Database(_options);
    }

    public bool TryGet(ReadOnlySpan<byte> key, out ReadOnlySpan<byte> value)
    {
        return _database.TryGet(key, out value);
    }

    public bool TrySet(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
    {
        return _database.TrySet(key, value);
    }

    public void WaitCompaction()
    {
        while (_database.IsCompacting)
        {
            Thread.Yield();
        }
    }

    public void Dispose()
    {
        _database.Dispose();
    }
}
