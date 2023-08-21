using Evdb.IO;
using System.Text;

namespace Evdb.Indexes.Lsm;

public sealed class WriteAheadLog : IDisposable
{
    private bool _disposed;

    private readonly Stream _file;
    private readonly BinaryWriter _writer;

    public WriteAheadLog(IFileSystem fs, string path)
    {
        ArgumentNullException.ThrowIfNull(fs, nameof(fs));

        _file = fs.OpenFile(path, FileMode.Create, FileAccess.Write);
        _writer = new BinaryWriter(_file, Encoding.UTF8, leaveOpen: true);
    }

    public void LogSet(in IndexKey ikey, in ReadOnlySpan<byte> value)
    {
        _writer.Write((byte)1);
        _writer.Write(ikey.Value);
        _writer.Write7BitEncodedInt((int)ikey.Version);
        _writer.Write7BitEncodedInt(value.Length);
        _writer.Write(value);
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _writer.Dispose();
        _file.Dispose();

        _disposed = true;
    }
}
