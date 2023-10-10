namespace Evdb.Indexing;

internal abstract class File
{
    public FileMetadata Metadata { get; }

    protected File(FileMetadata metadata)
    {
        Metadata = metadata;
    }
}
