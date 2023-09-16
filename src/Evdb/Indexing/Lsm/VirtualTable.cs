﻿using Evdb.Collections;
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
        if (Size > Capacity)
        {
            return false;
        }

        _wal.LogSet(key, value);
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
    public FileMetadata Flush(string path)
    {
        FileMetadata metadata = new(path, FileType.Table, Metadata.Id.Number);

        using (Stream file = _fs.OpenFile(metadata.Path, FileMode.Create, FileAccess.Write, FileShare.None))
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
