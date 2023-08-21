namespace Evdb.Indexes.Lsm;

public class FileMetadata
{
    public FileType Type { get; }
    public ulong Number { get; }
    public IndexKey? MinKey { get; }
    public IndexKey? MaxKey { get; }
    public string Path { get; }

    public FileId Id => new(Type, Number);

    public FileMetadata(string path, FileType type, ulong number, IndexKey? minKey = default, IndexKey? maxKey = default)
    {
        Type = type;
        Number = number;
        MinKey = minKey;
        MaxKey = maxKey;

        Path = Id.GetPath(path);
    }
}
