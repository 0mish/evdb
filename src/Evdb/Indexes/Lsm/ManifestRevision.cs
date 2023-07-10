using System.Collections.Immutable;

namespace Evdb.Indexes.Lsm;

public sealed class ManifestRevision
{
    public int ReferenceCount { get; private set; }

    public ulong VersionNumber { get; }
    public ulong FileNumber { get; }
    public ImmutableArray<FileMetadata> Files { get; }

    public ManifestRevision? Next { get; set; }
    public ManifestRevision? Previous { get; set; }

    public ManifestRevision(ulong versionNo, ulong fileNo, ImmutableArray<FileMetadata> files)
    {
        VersionNumber = versionNo;
        FileNumber = fileNo;
        Files = files;
    }

    public void Reference()
    {
        ReferenceCount++;
    }

    public void Unreference()
    {
        if (--ReferenceCount == 0)
        {
            // TODO: Dispose?
        }
    }
}
