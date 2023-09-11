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

    public bool TryGet(ReadOnlySpan<byte> key, out ReadOnlySpan<byte> value)
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

        for (iter.MoveToMin(); iter.Valid(); iter.MoveNext())
        {
            if (iter.Key.SequenceEqual(key))
            {
                value = iter.Value;

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

    public class Iterator : IIterator
    {
        private byte[]? _key;
        private byte[]? _value;

        private readonly BinaryReader _reader;
        private readonly long _dataPosition;

        public ReadOnlySpan<byte> Key => _key;
        public ReadOnlySpan<byte> Value => _value;

        public Iterator(BinaryReader reader, long position)
        {
            _reader = reader;
            _dataPosition = position;

            MoveToMin();
        }

        public bool Valid()
        {
            return _reader.BaseStream.Position < _reader.BaseStream.Length;
        }

        public void MoveToMin()
        {
            _reader.BaseStream.Seek(_dataPosition, SeekOrigin.Begin);

            MoveNext();
        }

        public void MoveTo(ReadOnlySpan<byte> key)
        {
            MoveToMin();

            while (Valid() && key.SequenceCompareTo(Key) < 0)
            {
                MoveNext();
            }
        }

        public void MoveNext()
        {
            _key = _reader.ReadByteArray();
            _value = _reader.ReadByteArray();
        }
    }
}
