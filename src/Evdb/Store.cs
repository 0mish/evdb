using Evdb.Indexing.Lsm;

namespace Evdb;

public struct RecordIterator
{

}

public sealed class RecordStream
{
    private readonly LsmIndex _index;
    private readonly string _name;

    internal RecordStream(LsmIndex index, string name)
    {
        _index = index;
        _name = name;
    }

    public void Append(in ReadOnlySpan<byte> value)
    {
        _index.TrySet(_name, value);
    }

    public RecordIterator Read()
    {
        throw new NotImplementedException();
    }
}

public sealed class Store : IDisposable
{
    private bool _disposed;
    private LsmIndex _index;

    public Store(string path)
    {
        _index = new LsmIndex(new LsmIndexOptions
        {
            Path = path
        });
    }

    public RecordStream All()
    {
        throw new NotImplementedException();
    }

    public RecordStream Get(string name)
    {
        ArgumentNullException.ThrowIfNull(name, nameof(name));

        return new RecordStream(_index, name);
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
