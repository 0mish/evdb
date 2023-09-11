using System.Text.Json;

namespace Evdb;

public readonly ref struct Record
{
    public Guid Guid { get; } = default;
    public ReadOnlySpan<byte> Type { get; }
    public ReadOnlySpan<byte> Data { get; }

    public Record(ReadOnlySpan<byte> type, ReadOnlySpan<byte> data)
    {
        Type = type;
        Data = data;
    }

    public BoxedRecord Box()
    {
        return new BoxedRecord(this);
    }

    public T? Deserialize<T>()
    {
        return JsonSerializer.Deserialize<T>(Data);
    }

    public static byte[] Encode(ReadOnlySpan<byte> type, ReadOnlySpan<byte> value)
    {
        return value.ToArray();
    }
}

public class BoxedRecord
{
    private readonly byte[] _type;
    private readonly byte[] _data;

    public Guid Guid { get; }
    public ReadOnlySpan<byte> Type => _type;
    public ReadOnlySpan<byte> Data => _data;

    internal BoxedRecord(Record record)
    {
        Guid = record.Guid;
        _type = record.Type.ToArray();
        _data = record.Data.ToArray();
    }

    public Record Unbox()
    {
        return new Record(Type, Data);
    }

    public T? Deserialize<T>()
    {
        return JsonSerializer.Deserialize<T>(Data);
    }
}
