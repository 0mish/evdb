namespace Evdb.Indexes.Lsm;

public readonly struct FileId : IEquatable<FileId>
{
    public FileType Type { get; }
    public ulong Number { get; }

    public FileId(FileType type, ulong number)
    {
        Type = type;
        Number = number;
    }

    public bool Equals(FileId other)
    {
        return other.Type == Type && Number == Number;
    }

    public override bool Equals(object? obj)
    {
        return obj is FileId fileId && Equals(fileId);
    }

    public static bool operator ==(FileId left, FileId right)
    {
        return left.Equals(right);
    }

    public static bool operator !=(FileId left, FileId right)
    {
        return !(left == right);
    }

    public override int GetHashCode()
    {
        return HashCode.Combine(Type, Number);
    }
}
