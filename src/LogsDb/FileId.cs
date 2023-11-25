using System.Diagnostics;

namespace LogsDb;

[DebuggerDisplay("{DebuggerDisplay,nq}")]
internal readonly struct FileId : IEquatable<FileId>
{
    private string DebuggerDisplay => $"FileId = {GetPath(string.Empty)}";

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

    public static bool TryParse(string value, out FileId result)
    {
        string numberStr = Path.GetFileNameWithoutExtension(value);

        if (!ulong.TryParse(numberStr, out ulong number))
        {
            result = default;

            return false;
        }

        string typeStr = Path.GetExtension(value);
        FileType type = typeStr switch
        {
            ".manifest" => FileType.Manifest,
            ".ulog" => FileType.Log,
            ".olog" => FileType.Table,
            _ => FileType.None
        };

        if (type == FileType.None)
        {
            result = default;

            return false;
        }

        result = new FileId(type, number);

        return true;
    }
}
