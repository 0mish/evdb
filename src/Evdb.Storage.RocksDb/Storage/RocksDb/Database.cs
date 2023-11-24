using RocksDbSharp;

namespace Evdb.Storage.RocksDb;

public sealed class Database : IDatabase
{
    private bool _disposed;

    private RocksDbSharp.RocksDb? _db;
    private readonly DatabaseOptions _options;

    public Database(DatabaseOptions options)
    {
        _options = options;
    }

    public Status Open()
    {
        DbOptions opt = new DbOptions()
            .SetCreateIfMissing(true)
            .SetWriteBufferSize(_options.WriteBufferSize)
            .SetDisableAutoCompactions(_options.AutoCompaction ? 0 : 1)
            .SetParanoidChecks(_options.ParanoidChecks);

        try
        {
            _db = RocksDbSharp.RocksDb.Open(opt, _options.Path);
        }
        catch (RocksDbException)
        {
            return Status.Failed;
        }

        return Status.Success;
    }

    public Status Get(ReadOnlySpan<byte> key, out ReadOnlySpan<byte> value)
    {
        value = default;

        if (_disposed)
        {
            return Status.Disposed;
        }
        else if (_db == null)
        {
            return Status.Closed;
        }

        try
        {
            byte[] result = _db.Get(key);

            value = result;

            return result == null ? Status.NotFound : Status.Found;
        }
        catch (RocksDbException)
        {
            return Status.Failed;
        }
    }

    public Status Set(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
    {
        value = default;

        if (_disposed)
        {
            return Status.Disposed;
        }
        else if (_db == null)
        {
            return Status.Closed;
        }

        try
        {
            _db.Put(key, value);

            return Status.Success;
        }
        catch (RocksDbException)
        {
            return Status.Failed;
        }
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _db?.Dispose();
        _db = null;

        _disposed = true;
    }
}
