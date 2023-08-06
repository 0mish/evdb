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

    public FileMetadata(string path, FileType type, ulong number, IndexKey? minKey = default, IndexKey? maxKey = default)
    {
        Type = type;
        Number = number;
        MinKey = minKey;
        MaxKey = maxKey;

        Path = type switch
        {
            FileType.Manifest => FileName.Manifest(path, number),
            FileType.Table => FileName.Table(path, number),
            FileType.Log => FileName.Log(path, number),
            _ => throw new NotImplementedException()
        };
    }

    public bool TryGetTable([MaybeNullWhen(false)] out PhysicalTable table)
    {
        table = default;

        return false;
    }
}
