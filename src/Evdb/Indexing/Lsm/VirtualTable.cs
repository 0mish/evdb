using Evdb.Collections;
using Evdb.IO;
using System.Diagnostics;
using System.Text;

namespace Evdb.Indexing.Lsm;

[DebuggerDisplay("{DebuggerDisplay,nq}")]
internal sealed class VirtualTable : File, IDisposable
{
    private string DebuggerDisplay => $"VirtualTable {Metadata.Path}";

    private bool _disposed;
    private readonly SkipList _kvs;
    private readonly WriteAheadLog _wal;
    private readonly IFileSystem _fs;

    public long Size { get; private set; }
    public long Capacity { get; }

    public VirtualTable(IFileSystem fs, FileMetadata metadata, long capacity) : base(metadata)
    {
        ArgumentNullException.ThrowIfNull(fs, nameof(fs));

        Capacity = capacity;

        _kvs = new SkipList();
        _wal = new WriteAheadLog(fs, metadata.Path);
        _fs = fs;
    }

    public bool TrySet(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
    {
        long newSize = Size + key.Length + value.Length;

        if (newSize > Capacity)
        {
            return false;
        }

        _wal.LogSet(key, value);
        _kvs.Set(key, value);

        Size = newSize;

        return true;
    }

    public bool TryGet(ReadOnlySpan<byte> key, out ReadOnlySpan<byte> value)
    {
        return _kvs.TryGet(key, out value);
    }

    public Iterator GetIterator()
    {
        return new Iterator(_kvs.GetIterator());
    }

    // TODO: Consider empty tables.
    public FileMetadata Flush(string path)
    {
        FileMetadata metadata = new(path, FileType.Table, Metadata.Id.Number);

        using (Stream file = _fs.OpenFile(metadata.Path, FileMode.Create, FileAccess.Write))
        using (BinaryWriter writer = new(file, Encoding.UTF8, leaveOpen: true))
        {
            BloomFilter filter = new(size: 4096);
            SkipList.Iterator iter = _kvs.GetIterator();

            iter.MoveToMin();

            while (iter.TryMoveNext(out ReadOnlySpan<byte> key, out _))
            {
                filter.Set(key);
            }

            _kvs.TryGetMin(out ReadOnlySpan<byte> minKey, out _);
            _kvs.TryGetMax(out ReadOnlySpan<byte> maxKey, out _);

            writer.WriteByteArray(filter.Buffer);
            writer.WriteByteArray(minKey);
            writer.WriteByteArray(maxKey);

            iter.MoveToMin();

            while (iter.TryMoveNext(out ReadOnlySpan<byte> key, out ReadOnlySpan<byte> value))
            {
                writer.WriteByteArray(key);
                writer.WriteByteArray(value);
            }
        }

        return metadata;
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _wal.Dispose();
        _disposed = true;
    }

    public struct Iterator
    {
        private SkipList.Iterator _iter;

        public Iterator(SkipList.Iterator iter)
        {
            _iter = iter;
        }

        public void MoveToMin()
        {
            _iter.MoveToMin();
        }

        public void MoveTo(ReadOnlySpan<byte> key)
        {
            _iter.MoveTo(key);
        }

        public bool TryMoveNext(out ReadOnlySpan<byte> key, out ReadOnlySpan<byte> value)
        {
            return _iter.TryMoveNext(out key, out value);
        }
    }
}
