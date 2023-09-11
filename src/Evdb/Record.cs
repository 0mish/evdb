using System.Text.Json;

namespace Evdb;

public readonly ref struct Record
{
    public ReadOnlySpan<byte> Type { get; }
    public ReadOnlySpan<byte> Data { get; }

    public Record(ReadOnlySpan<byte> type, ReadOnlySpan<byte> data)
    {
        Type = type;
        Data = data;
    }

    public T? Decode<T>()
    {
        return JsonSerializer.Deserialize<T>(Data);
    }
}
