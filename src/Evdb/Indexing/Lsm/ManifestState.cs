using System.Collections.Immutable;

namespace Evdb.Indexing.Lsm;

internal sealed class ManifestState
{
    public int ReferenceCount { get; private set; }

    public ulong VersionNumber { get; }
    public ulong FileNumber { get; }
    public ImmutableArray<FileId> Files { get; }

    public ManifestState? Next { get; set; }
    public ManifestState? Previous { get; set; }

    public ManifestState(ulong versionNo, ulong fileNo, ImmutableArray<FileId> files)
    {
        VersionNumber = versionNo;
        FileNumber = fileNo;
        Files = files;

        ReferenceCount = 1;
    }

    public void Reference()
    {
        ReferenceCount++;
    }

    public bool Unreference()
    {
        return --ReferenceCount == 0;
    }
}
