namespace Evdb.Indexing.Lsm;

internal abstract class File
{
    public FileMetadata Metadata { get; }

    protected File(FileMetadata metadata)
    {
        ArgumentNullException.ThrowIfNull(metadata, nameof(metadata));

        Metadata = metadata;
    }
}
