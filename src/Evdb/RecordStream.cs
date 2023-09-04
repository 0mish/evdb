using Evdb.Indexing.Lsm;
using System.Buffers.Binary;
using System.Text;
using System.Text.Json;

namespace Evdb;

public sealed class RecordStream
{
    private bool _hasCount;
    private uint _count;

    private readonly object _sync;
    private readonly byte[] _key;
    private readonly LsmIndex _index;

    internal RecordStream(LsmIndex index, string name)
    {
        _sync = new object();
        _index = index;
        _key = Encoding.UTF8.GetBytes(name);
    }

    public void Append(in ReadOnlySpan<byte> value)
    {
        lock (_sync)
        {
            if (!_hasCount)
            {
                // FIXME: Find the max key prefixed with "_key".
                _count = 0;
                _hasCount = true;
            }

            Span<byte> key = EncodeKey(_count++);

            _index.TrySet(key, value);
        }
    }

    public void AppendJson<T>(T value)
    {
        using MemoryStream stream = new();

        JsonSerializer.Serialize(stream, value);

        ReadOnlySpan<byte> buffer = stream.GetBuffer();
        ReadOnlySpan<byte> json = buffer[..(int)stream.Length];

        Append(json);
    }

    public Iterator GetIterator()
    {
        throw new NotImplementedException();
    }

    // FIXME: optimize - remove unnecessary allocations and remove bound checks.
    private byte[] EncodeKey(uint count)
    {
        byte[] result = new byte[_key.Length + sizeof(uint)];

        _key.AsSpan().CopyTo(result);
        BinaryPrimitives.WriteUInt32LittleEndian(result.AsSpan().Slice(_key.Length), count);

        return result;
    }

    public struct Iterator
    {

    }
}
