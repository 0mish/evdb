using System.Diagnostics.CodeAnalysis;

namespace Evdb.Indexes.Lsm;

public class FileMetadata
{
    public FileType Type { get; }
    public ulong Number { get; }
    public IndexKey? MinKey { get; }
    public IndexKey? MaxKey { get; }

    public string Path { get; set; } = default!;

    public FileId Id => new(Type, Number);

    public FileMetadata(FileType type, ulong number, IndexKey? minKey = default, IndexKey? maxKey = default)
    {
        Type = type;
        Number = number;
        MinKey = minKey;
        MaxKey = maxKey;

        // FIXME.
        Path = type switch
        {
            FileType.Manifest => FileName.Manifest("db", number),
            FileType.Table => FileName.Table("db", number),
            FileType.Log => FileName.Log("db", number),
            _ => throw new NotImplementedException()
        };
    }

    public bool TryGetTable([MaybeNullWhen(false)] out PhysicalTable table)
    {
        table = default;

        return false;
    }
}
