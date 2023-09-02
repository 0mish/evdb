using Evdb.Indexing.Lsm;
using System.Runtime.InteropServices;
using System.Text.Json;

namespace Evdb;

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
            _index.TrySet(MemoryMarshal.Cast<char, byte>(_name.AsSpan()), value);
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

    public RecordStreamIterator Iterator()
    {
        throw new NotImplementedException();
    }
}
