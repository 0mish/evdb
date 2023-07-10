namespace Evdb.Indexes;

public readonly struct IndexKey : IComparable<IndexKey>
{
    public string Value { get; }
    public ulong Version { get; }

    public IndexKey(string value, ulong version)
    {
        Value = value;
        Version = version;
    }

    public int CompareTo(IndexKey other)
    {
        int result = Value.CompareTo(other.Value);

        return result != 0 ? result : Version.CompareTo(other.Version);
    }

    public override string ToString()
    {
        return $"\"{Value}\":{Version}";
    }

    public override int GetHashCode()
    {
        return HashCode.Combine(Value, Version);
    }
}
