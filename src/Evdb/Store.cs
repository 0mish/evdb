using Evdb.Indexing.Lsm;
using System.Collections.Concurrent;

namespace Evdb;

public struct RecordStreamIterator
{
    public bool TryGetNext(out ReadOnlySpan<byte> value)
    {
        throw new NotImplementedException();
    }
}

public sealed class RecordStream
{
    private readonly object _sync;
    private readonly LsmIndex _index;
    private readonly string _name;

    internal RecordStream(LsmIndex index, string name)
    {
        _sync = new object();
        _index = index;
        _name = name;
    }

    public void Append(in ReadOnlySpan<byte> value)
    {
        lock (_sync)
        {
            _index.TrySet(_name, value);
        }
    }

    public RecordStreamIterator Iterator()
    {
        throw new NotImplementedException();
    }
}

public sealed class Store : IDisposable
{
    private bool _disposed;
    private readonly LsmIndex _index;
    private readonly ConcurrentDictionary<string, RecordStream> _streams;

    public Store(string path)
    {
        LsmIndexOptions options = new()
        {
            Path = path
        };

        _index = new LsmIndex(options);
        _streams = new ConcurrentDictionary<string, RecordStream>();
    }

    public RecordStream All()
    {
        throw new NotImplementedException();
    }

    public RecordStream Get(string name)
    {
        ArgumentNullException.ThrowIfNull(name, nameof(name));

        return _streams.GetOrAdd(name, key => new RecordStream(_index, name));
    }

    public bool Exists(string name)
    {
        ArgumentNullException.ThrowIfNull(name, nameof(name));

        return _index.TryGet(name, out _);
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _index.Dispose();
        _disposed = true;
    }
}
