using Evdb.IO;
using System.Diagnostics;

namespace Evdb.Indexing.Lsm;

[DebuggerDisplay("{DebuggerDisplay,nq}")]
internal sealed class PhysicalTable : File, IDisposable
{
    private string DebuggerDisplay => $"PhysicalTable {Metadata.Path}";

    private bool _disposed;

    private readonly BloomFilter _filter;
    private readonly byte[] _maxKey;
    private readonly byte[] _minKey;
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
        _minKey = _reader.ReadByteArray();
        _maxKey = _reader.ReadByteArray();

        _dataPosition = _file.Position;
    }

    public bool TryGet(ReadOnlySpan<byte> key, out ReadOnlySpan<byte> value, ulong version)
    {
        // If not in range of keys in the table, we exit early.
        if (key.SequenceCompareTo(_minKey) < 0 || key.SequenceCompareTo(_maxKey) > 0)
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

        // Otherwise we perform the look up in the file.
        Iterator iter = GetIterator();

        while (iter.TryMoveNext(out ReadOnlySpan<byte> fileKey, out ReadOnlySpan<byte> fileValue))
        {
            if (fileKey.SequenceEqual(key))
            {
                value = fileValue;

                return true;
            }
        }

        value = default;

        return false;
    }

    public Iterator GetIterator()
    {
        return new Iterator(_reader, _dataPosition);
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

    public readonly struct Iterator
    {
        private readonly BinaryReader _reader;
        private readonly long _dataPosition;

        public Iterator(BinaryReader reader, long position)
        {
            _reader = reader;
            _dataPosition = position;

            MoveToMin();
        }

        public readonly void MoveToMin()
        {
            _reader.BaseStream.Seek(_dataPosition, SeekOrigin.Begin);
        }

        public readonly bool TryMoveNext(out ReadOnlySpan<byte> key, out ReadOnlySpan<byte> value)
        {
            if (_reader.BaseStream.Position < _reader.BaseStream.Length)
            {
                key = _reader.ReadByteArray();
                value = _reader.ReadByteArray();

                return true;
            }

            key = default;
            value = default;

            return false;
        }
    }
}
