using Evdb.Collections;
using Evdb.IO;
using System.Diagnostics;
using System.Text;

namespace Evdb.Indexing;

[DebuggerDisplay("{DebuggerDisplay,nq}")]
internal sealed class PhysicalTable : File, IDisposable
{
    private string DebuggerDisplay => $"PhysicalTable {Metadata.Path}";

    private bool _disposed;
    private readonly IFileSystem _fs;
    private readonly BloomFilter _filter;
    private readonly byte[] _firstKey;
    private readonly byte[] _lastKey;
    private readonly long _position;

    public PhysicalTable(IFileSystem fs, FileMetadata metadata) : base(metadata)
    {
        _fs = fs;

        using Stream file = fs.OpenFile(metadata.Path, FileMode.Open, FileAccess.Read, FileShare.Read);
        using BinaryReader reader = new(file);

        _filter = new BloomFilter(reader.ReadByteArray());
        _firstKey = reader.ReadByteArray();
        _lastKey = reader.ReadByteArray();
        _position = file.Position;
    }

    public bool TryGet(ReadOnlySpan<byte> key, out ReadOnlySpan<byte> value)
    {
        // If not in range of keys in the table, we exit early.
        if (key.SequenceCompareTo(_firstKey) < 0 || key.SequenceCompareTo(_lastKey) > 0)
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
        using Iterator iter = GetIterator();

        for (iter.MoveToFirst(); iter.Valid(); iter.MoveNext())
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
        return new Iterator(_fs, Metadata, _position);
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
    }

    public sealed class Iterator : IIterator
    {
        private bool _disposed;

        private byte[]? _key;
        private byte[]? _value;

        private readonly BinaryReader _reader;
        private readonly long _position;

        public ReadOnlySpan<byte> Key => _key;
        public ReadOnlySpan<byte> Value => _value;

        public Iterator(IFileSystem fs, FileMetadata metadata, long position)
        {
            // TODO: optimize - Instead of creating a new file handle each, we can pool them instead.
            Stream file = fs.OpenFile(metadata.Path, FileMode.Open, FileAccess.Read, FileShare.Read);

            _reader = new BinaryReader(file, Encoding.UTF8, leaveOpen: false);
            _position = position;

            MoveToFirst();
        }

        public bool Valid()
        {
            return !_disposed && _key != null && _value != null;
        }

        public void MoveToFirst()
        {
            _reader.BaseStream.Seek(_position, SeekOrigin.Begin);

            MoveNext();
        }

        public void MoveTo(ReadOnlySpan<byte> key)
        {
            MoveToFirst();

            while (Valid() && key.SequenceCompareTo(Key) < 0)
            {
                MoveNext();
            }
        }

        public void MoveNext()
        {
            if (_reader.BaseStream.Position < _reader.BaseStream.Length)
            {
                _key = _reader.ReadByteArray();
                _value = _reader.ReadByteArray();
            }
            else
            {
                _key = null;
                _value = null;
            }
        }

        public void Dispose()
        {
            if (_disposed)
            {
                return;
            }

            _reader.Dispose();
            _disposed = true;
        }
    }
}
