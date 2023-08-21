namespace Evdb.Indexes.Lsm;

public class FileMetadata
{
    public FileId Id { get; }
    public string Path { get; }

    public FileMetadata(string path, FileType type, ulong number)
    {
        Id = new FileId(type, number);
        Path = Id.GetPath(path);
    }
}
