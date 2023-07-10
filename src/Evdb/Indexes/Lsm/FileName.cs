namespace Evdb.Indexes.Lsm;

public static class FileName
{
    public static string Manifest(string path, ulong number)
    {
        return Path.Join(path, $"{number:D6}.manifest");
    }

    public static string Log(string path, ulong number)
    {
        return Path.Join(path, $"{number:D6}.ulog");
    }

    public static string Table(string path, ulong number)
    {
        return Path.Join(path, $"{number:D6}.olog");
    }
}
