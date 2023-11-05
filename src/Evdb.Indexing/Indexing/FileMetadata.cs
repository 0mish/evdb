namespace Evdb.Indexing;

internal sealed class FileMetadata
{
    public FileId Id { get; }
    public string Path { get; }

    public FileMetadata(string path, FileType type, ulong number)
    {
        Id = new FileId(type, number);
        Path = Id.GetPath(path);
    }
}
