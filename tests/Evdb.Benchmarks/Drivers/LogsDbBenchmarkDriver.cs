using Evdb.Storage.LogsDb;

namespace Evdb.Benchmarks.Drivers;

public struct EvdbBenchmarkDriverOptions
{
    public int VirtualTableSize { get; set; }
}

public readonly struct LogsDbBenchmarkDriver : IBenchmarkDriver, IDisposable
{
    private readonly Database _database;
    private readonly DatabaseOptions _options;

    public LogsDbBenchmarkDriver(EvdbBenchmarkDriverOptions options)
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
        _database.Open();
    }

    public bool TryGet(ReadOnlySpan<byte> key, out ReadOnlySpan<byte> value)
    {
        return _database.Get(key, out value).IsFound;
    }

    public bool TrySet(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
    {
        return _database.Set(key, value).IsFound;
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
