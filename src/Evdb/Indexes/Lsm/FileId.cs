using System.Diagnostics;

namespace Evdb.Indexes.Lsm;

[DebuggerDisplay("FileId {DebuggerDisplay,nq}")]
public readonly struct FileId : IEquatable<FileId>
{
    private string DebuggerDisplay => $"{GetPath(string.Empty)}";

    public FileType Type { get; }
    public ulong Number { get; }

    public FileId(FileType type, ulong number)
    {
        Type = type;
        Number = number;
    }
    
    public string GetPath(string path)
    {
        return Type switch
        {
            FileType.Manifest => Path.Join(path, $"{Number:D6}.manifest"),
            FileType.Log => Path.Join(path, $"{Number:D6}.ulog"),
            FileType.Table => Path.Join(path, $"{Number:D6}.olog"),
            _ => throw new NotSupportedException()
        };
    }

    public bool Equals(FileId other)
    {
        return other.Type == Type && other.Number == Number;
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
