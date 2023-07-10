namespace Evdb.Indexes.Lsm;

public struct ManifestEdit
{
    public ulong? VersionNumber { get; set; }
    public ulong? FileNumber { get; set; }
    public List<FileId>? FilesRegistered { get; set; }
    public List<FileId>? FilesUnregistered { get; set; }
}
