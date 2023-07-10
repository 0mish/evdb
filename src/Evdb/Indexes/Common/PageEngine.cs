namespace Evdb.Indexes.Common;

public sealed class PageEngine : IDisposable
{
    private const int PageSize = 4 * 1024;

    private bool _disposed;

    private int _number;
    private readonly FileStream _file;

    public string Path { get; }

    public PageEngine(string path)
    {
        ArgumentNullException.ThrowIfNull(path, nameof(path));

        _file = File.Open(path, FileMode.OpenOrCreate);

        Path = path;
    }

    public Page Allocate()
    {
        byte[] data = new byte[PageSize];
        int number = _number++;

        return new Page(number, data);
    }

    public bool Exists(int number)
    {
        return PageSize * number < _file.Length;
    }

    public Page Load(int number)
    {
        byte[] data = new byte[PageSize];

        _file.Seek(PageSize * number, SeekOrigin.Begin);
        _file.Read(data);

        return new Page(number, data);
    }

    public void Save(Page page)
    {
        int position = PageSize * page.Number;

        if (position > _file.Length)
        {
            _file.SetLength(position + PageSize);
        }

        _file.Seek(position, SeekOrigin.Begin);
        _file.Write(page.Span);
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _file.Dispose();

        _disposed = true;
    }
}
