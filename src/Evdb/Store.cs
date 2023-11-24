using Evdb.Storage;
using System.Collections.Concurrent;
using System.Text;

namespace Evdb;

public sealed class Store : IDisposable
{
    private bool _disposed;
    private readonly IDatabase _db;
    private readonly ConcurrentDictionary<string, RecordStream> _streams;

    public Store(StoreOptions options)
    {
        ArgumentNullException.ThrowIfNull(options, nameof(options));

        _streams = new ConcurrentDictionary<string, RecordStream>();
        _db = options.Database;
        _db.Open();
    }

    public RecordStream All()
    {
        throw new NotImplementedException();
    }

    public RecordStream Get(string name)
    {
        ArgumentNullException.ThrowIfNull(name, nameof(name));

        return _streams.GetOrAdd(name, key => new RecordStream(_db, name));
    }

    public bool Exists(string name)
    {
        ArgumentNullException.ThrowIfNull(name, nameof(name));

        ReadOnlySpan<byte> key = Encoding.UTF8.GetBytes(name);

        return _db.Get(key, out _).IsSuccess;
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _db.Dispose();
        _disposed = true;
    }
}
