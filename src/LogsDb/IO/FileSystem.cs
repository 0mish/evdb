namespace LogsDb.IO;

public interface IFileSystem
{
    void CreateDirectory(string path);
    FileStream OpenFile(string path, FileMode mode, FileAccess access, FileShare share);
    string[] ListFiles(string path);
    bool DeleteFile(string path);
}

public sealed class FileSystem : IFileSystem
{
    public void CreateDirectory(string path)
    {
        Directory.CreateDirectory(path);
    }

    public FileStream OpenFile(string path, FileMode mode, FileAccess access, FileShare share)
    {
        return System.IO.File.Open(path, mode, access, share);
    }

    public string[] ListFiles(string path)
    {
        return Directory.GetFiles(path);
    }

    public bool DeleteFile(string path)
    {
        try { System.IO.File.Delete(path); }
        catch { return false; }

        return true;
    }
}
