namespace Evdb.Indexes.Lsm;

public readonly struct FileId
{
    public FileType Type { get; }
    public ulong Number { get; }

    public FileId(FileType type, ulong number)
    {
        Type = type;
        Number = number;
    }
}
