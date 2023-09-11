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

    public T? Deserialize<T>()
    {
        return JsonSerializer.Deserialize<T>(Data);
    }

    public static byte[] Encode(ReadOnlySpan<byte> type, ReadOnlySpan<byte> value)
    {
        return value.ToArray();
    }
}
