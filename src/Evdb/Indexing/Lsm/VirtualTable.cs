using Evdb.Collections;
using Evdb.IO;
using System.Diagnostics;
using System.Text;

namespace Evdb.Indexing.Lsm;

[DebuggerDisplay("{DebuggerDisplay,nq}")]
internal sealed class VirtualTable : IDisposable
{
    private string DebuggerDisplay => $"VirtualTable {_log.Metadata.Path}";

    private bool _disposed;
    private readonly SkipList _kvs;
    private readonly PhysicalLog _log;

    public long Size { get; private set; }
    public long Capacity { get; }

    public VirtualTable(PhysicalLog log, long capacity)
    {
        Capacity = capacity;

        _log = log;
        _kvs = new SkipList();
    }

    public bool TrySet(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
    {
        if (Size > Capacity)
        {
            return false;
        }

        _log.LogSet(key, value);
        _kvs.Set(key, value);

        Size += key.Length + value.Length;

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
    public PhysicalTable Flush(IFileSystem fs, string path)
    {
        FileMetadata metadata = new(path, FileType.Table, _log.Metadata.Id.Number);

        using (Stream file = fs.OpenFile(metadata.Path, FileMode.Create, FileAccess.Write, FileShare.None))
        using (BinaryWriter writer = new(file, Encoding.UTF8, leaveOpen: true))
        {
            BloomFilter filter = new(size: 4096);
            SkipList.Iterator iter = _kvs.GetIterator();

            for (iter.MoveToFirst(); iter.Valid(); iter.MoveNext())
            {
                filter.Set(iter.Key);
            }

            _kvs.TryGetFirst(out ReadOnlySpan<byte> firstKey, out _);
            _kvs.TryGetLast(out ReadOnlySpan<byte> lastKey, out _);

            writer.WriteByteArray(filter.Buffer);
            writer.WriteByteArray(firstKey);
            writer.WriteByteArray(lastKey);

            for (iter.MoveToFirst(); iter.Valid(); iter.MoveNext())
            {
                writer.WriteByteArray(iter.Key);
                writer.WriteByteArray(iter.Value);
            }
        }

        return new PhysicalTable(fs, metadata);
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        // _kvs.Dispose()
        _disposed = true;
    }

    public sealed class Iterator : IIterator
    {
        private SkipList.Iterator _iter;

        public ReadOnlySpan<byte> Key => _iter.Key;
        public ReadOnlySpan<byte> Value => _iter.Value;

        public Iterator(SkipList.Iterator iter)
        {
            _iter = iter;
        }

        public bool Valid()
        {
            return _iter.Valid();
        }

        public void MoveToFirst()
        {
            _iter.MoveToFirst();
        }

        public void MoveTo(ReadOnlySpan<byte> key)
        {
            _iter.MoveTo(key);
        }

        public void MoveNext()
        {
            _iter.MoveNext();
        }

        public void Dispose()
        {

        }
    }
}
