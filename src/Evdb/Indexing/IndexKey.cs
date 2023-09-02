using System.Text;

namespace Evdb.Indexing;

internal readonly struct IndexKey : IComparable<IndexKey>
{
    public byte[] Value { get; }
    public ulong Version { get; }

    public IndexKey(byte[] value, ulong version)
    {
        Value = value;
        Version = version;
    }

    public int CompareTo(IndexKey other)
    {
        int result = Value.AsSpan().SequenceCompareTo(other.Value);

        return result != 0 ? result : Version.CompareTo(other.Version);
    }

    public override string ToString()
    {
        return $"\"{Encoding.UTF8.GetString(Value)}\":{Version}";
    }

    public override int GetHashCode()
    {
        return HashCode.Combine(Value, Version);
    }
}
