using Evdb.Indexing.Lsm;
using System.Text;
using System.Text.Json;

namespace Evdb;

public sealed class RecordStream
{
    private bool _hasCount;
    private int _count;

    private readonly object _sync;
    private readonly byte[] _key;
    private readonly LsmIndex _index;

    internal RecordStream(LsmIndex index, string name)
    {
        _sync = new object();
        _index = index;
        _key = Encoding.UTF8.GetBytes(name);
    }

    public void Append(string type, ReadOnlySpan<byte> json)
    {
        lock (_sync)
        {
            if (!_hasCount)
            {
                // FIXME: Find the max key prefixed with "_key".
                _count = 0;
                _hasCount = true;
            }

            ReadOnlySpan<byte> key = RecordKey.Encode(_key, _count++);

            _index.TrySet(key, json);
        }
    }

    public void Append<T>(string type, T value)
    {
        using MemoryStream stream = new();

        JsonSerializer.Serialize(stream, value);

        ReadOnlySpan<byte> buffer = stream.GetBuffer();
        ReadOnlySpan<byte> json = buffer[..(int)stream.Length];

        Append(type, json);
    }

    public Iterator GetIterator()
    {
        return new Iterator();
    }

    public struct Iterator
    {
        public bool TryMoveNext(out Record record)
        {
            record = default;

            return false;
        }
    }
}
