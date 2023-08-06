namespace Evdb.Indexes.Lsm;

public struct ManifestEdit
{
    public ulong? VersionNumber { get; set; }
    public ulong? FileNumber { get; set; }
    public FileId[]? FilesRegistered { get; set; }
    public FileId[]? FilesUnregistered { get; set; }
}
