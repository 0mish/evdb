using Evdb.Collections;
using Evdb.IO;
using System.Diagnostics;

namespace Evdb.Indexing;

[DebuggerDisplay("{DebuggerDisplay,nq}")]
internal sealed class VirtualTable : IDisposable
{
    private string DebuggerDisplay => $"VirtualTable {_log?.Metadata.Path}";

    private bool _disposed;
    private readonly SkipList _kvs;
    private readonly PhysicalLog? _log;

    public long Size { get; private set; }
    public long Capacity { get; }

    public VirtualTable(PhysicalLog? log, long capacity)
    {
        Capacity = capacity;

        _log = log;
        _kvs = new SkipList();
    }

    public bool TrySet(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
    {
        if (_disposed || Size > Capacity)
        {
            return false;
        }

        _log?.LogSet(key, value);
        _kvs.Set(key, value);

        Size += key.Length + value.Length;

        return true;
    }

    public bool TryGet(ReadOnlySpan<byte> key, out ReadOnlySpan<byte> value)
    {
        if (_disposed)
        {
            value = default;

            return false;
        }

        return _kvs.TryGet(key, out value);
    }

    public Iterator GetIterator()
    {
        return new Iterator(_kvs.GetIterator());
    }

    public void Flush(IFileSystem fs, FileMetadata metadata)
    {
        using Stream file = fs.OpenFile(metadata.Path, FileMode.Create, FileAccess.Write, FileShare.None);
        using PhysicalTableBuilder builder = new(file, leaveOpen: true);

        SkipList.Iterator iter = _kvs.GetIterator();

        for (iter.MoveToFirst(); iter.IsValid; iter.MoveNext())
        {
            builder.Add(iter.Key, iter.Value);
        }
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
        public bool IsValid => _iter.IsValid;

        public Iterator(SkipList.Iterator iter)
        {
            _iter = iter;
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
