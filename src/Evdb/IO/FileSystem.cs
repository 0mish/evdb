namespace Evdb.IO;

public interface IFileSystem
{
    void CreateDirectory(string path);
    Stream OpenFile(string path, FileMode mode, FileAccess access);
    string[] ListFiles(string path);
    void DeleteFile(string path);
}

public class FileSystem : IFileSystem
{
    public void CreateDirectory(string path)
    {
        Directory.CreateDirectory(path);
    }

    public Stream OpenFile(string path, FileMode mode, FileAccess access)
    {
        return File.Open(path, mode, access);
    }

    public string[] ListFiles(string path)
    {
        return Directory.GetFiles(path);
    }

    public void DeleteFile(string path)
    {
        File.Delete(path);
    }
}
