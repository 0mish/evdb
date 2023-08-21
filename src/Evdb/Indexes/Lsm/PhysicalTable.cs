using Evdb.Indexes.Common;
using Evdb.IO;
using System.Diagnostics;

namespace Evdb.Indexes.Lsm;

[DebuggerDisplay("{DebuggerDisplay,nq}")]
public sealed class PhysicalTable : File, IDisposable
{
    private string DebuggerDisplay => $"PhysicalTable {Metadata.Path}";

    private bool _disposed;

    private readonly BloomFilter _filter;
    private readonly string _maxKey;
    private readonly string _minKey;
    private readonly long _dataPosition;

    private readonly Stream _file;
    private readonly BinaryReader _reader;

    public PhysicalTable(IFileSystem fs, FileMetadata metadata) : base(metadata)
    {
        ArgumentNullException.ThrowIfNull(fs, nameof(fs));

        _file = fs.OpenFile(metadata.Path, FileMode.Open, FileAccess.Read);
        _file.Seek(0, SeekOrigin.Begin);

        _reader = new BinaryReader(_file);

        int filterSize = _reader.Read7BitEncodedInt();
        byte[] filterBuffer = _reader.ReadBytes(filterSize);

        _filter = new BloomFilter(filterBuffer);
        _minKey = _reader.ReadString();
        _maxKey = _reader.ReadString();

        _dataPosition = _file.Position;
    }

    public bool TryGet(IndexKey ikey, out ReadOnlySpan<byte> value)
    {
        string key = ikey.Value;

        // If not in range of keys in the table, we exit early.
        if (Comparer<string>.Default.Compare(key, _minKey) < 0 || Comparer<string>.Default.Compare(key, _maxKey) > 0)
        {
            value = default;

            return false;
        }

        // If not in filter, we exit early.
        if (!_filter.Test(key))
        {
            value = default;

            return false;
        }

        _file.Seek(_dataPosition, SeekOrigin.Begin);

        // Otherwise we perform the look up in the file.
        while (_file.Position < _file.Length)
        {
            string fileKey = _reader.ReadString();
            int fileValueLength = _reader.Read7BitEncodedInt();

            if (fileKey == key)
            {
                value = _reader.ReadBytes(fileValueLength);

                return true;
            }

            _file.Seek(fileValueLength, SeekOrigin.Current);
        }

        value = default;

        return false;
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _reader.Dispose();
        _file.Dispose();

        _disposed = true;
    }
}
