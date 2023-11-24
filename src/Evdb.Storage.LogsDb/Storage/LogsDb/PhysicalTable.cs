using Evdb.Collections;
using Evdb.Storage.LogsDb.Format;
using Evdb.IO;
using System.Diagnostics;

namespace Evdb.Storage.LogsDb;

[DebuggerDisplay("{DebuggerDisplay,nq}")]
internal sealed class PhysicalTable : File, IDisposable
{
    private string DebuggerDisplay => $"{nameof(PhysicalTable)} = {Metadata.Path}";

    private bool _disposed;

    private FileStream? _file;
    private BloomFilter? _filter;
    private Block? _index;
    private Footer _footer;

    private readonly IFileSystem _fs;
    private readonly IBlockCache _blockCache;

    public PhysicalTable(IFileSystem fs, FileMetadata metadata, IBlockCache blockCache) : base(metadata)
    {
        _fs = fs;
        _blockCache = blockCache;
    }

    public void Open()
    {
        _file = _fs.OpenFile(Metadata.Path, FileMode.Open, FileAccess.Read, FileShare.None);

        _footer = ReadFooter();
        _filter = ReadFilter();
        _index = ReadIndex();
    }

    public Status Get(ReadOnlySpan<byte> key, out ReadOnlySpan<byte> value)
    {
        value = default;

        if (_disposed)
        {
            return Status.Disposed;
        }

        // If not in range of keys in the table, we exit early.
        if (key.SequenceCompareTo(_footer.FirstKey) < 0 || key.SequenceCompareTo(_footer.LastKey) > 0)
        {
            return Status.NotFound;
        }

        // If not in filter, we exit early.
        if (_filter != null && !_filter.Test(key))
        {
            return Status.NotFound;
        }

        // Otherwise we perform the look up in the file.
        using Iterator iter = GetIterator();

        iter.MoveTo(key);

        if (iter.IsValid && iter.Key.SequenceEqual(key))
        {
            value = iter.Value;

            return Status.Found;
        }

        return Status.NotFound;
    }

    public Iterator GetIterator()
    {
        return new Iterator(this);
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _file?.Dispose();

        _disposed = true;
    }

    private Block? ReadBlock(BlockHandle handle)
    {
        if (handle.Position >= (ulong)_file!.Length || handle.Position + handle.Length > (ulong)_file.Length)
        {
            return null;
        }

        if (_blockCache.TryGet(Metadata.Id, handle, out Block? block))
        {
            return block;
        }

        byte[] data = new byte[handle.Length];

        // If stream is a FileStream, try to read directly without locks.
        if (_file is FileStream fileStream)
        {
            RandomAccess.Read(fileStream.SafeFileHandle, data, (long)handle.Position);
        }
        else
        {
            lock (_file)
            {
                _file.Seek((long)handle.Position, SeekOrigin.Begin);
                _file.Read(data, 0, data.Length);
            }
        }

        block = new Block(data);

        _blockCache.Set(Metadata.Id, handle, block);

        return block;
    }

    private Block ReadIndex()
    {
        return ReadBlock(_footer.IndexBlock) ?? throw new Exception("Failed to read index of physical table.");
    }

    private BloomFilter ReadFilter()
    {
        return new BloomFilter(_footer.Filter);
    }

    private Footer ReadFooter()
    {
        byte[] footerBuffer = new byte[4];

        _file!.Seek(-sizeof(int), SeekOrigin.End);
        _file.Read(footerBuffer);

        BinaryDecoder footerDecoder = new(footerBuffer);
        footerDecoder.UInt32(out uint footerLength);

        byte[] footerFullBuffer = new byte[footerLength];

        _file.Seek(-sizeof(int) - footerLength, SeekOrigin.End);
        _file.Read(footerFullBuffer);

        BinaryDecoder footerFullDecoder = new(footerFullBuffer);
        footerFullDecoder.ByteArray(out ArraySegment<byte> filter);
        footerFullDecoder.ByteArray(out ArraySegment<byte> firstKey);
        footerFullDecoder.ByteArray(out ArraySegment<byte> lastKey);

        return new Footer
        {
            Filter = filter.ToArray(),
            FirstKey = firstKey.ToArray(),
            LastKey = lastKey.ToArray(),
            IndexBlock = BlockHandle.Read(ref footerFullDecoder)
        };
    }

    private struct Footer
    {
        public byte[] Filter { get; set; }
        public byte[] FirstKey { get; set; }
        public byte[] LastKey { get; set; }
        public BlockHandle IndexBlock { get; set; }
    }

    public sealed class Iterator : IIterator
    {
        private bool _disposed;

        private Block.Iterator? _dataIterator;
        private readonly Block.Iterator _indexIterator;
        private readonly PhysicalTable _table;

        public ReadOnlySpan<byte> Key => _dataIterator!.Key;
        public ReadOnlySpan<byte> Value => _dataIterator!.Value;
        public bool IsValid => !_disposed && _indexIterator.IsValid;

        public Iterator(PhysicalTable table)
        {
            _table = table;
            _indexIterator = table._index!.GetIterator(); // FIXME: This can throw if we do not open the table.
        }

        public void MoveToFirst()
        {
            _indexIterator.MoveToFirst();

            InitDataIterator();

            _dataIterator?.MoveToFirst();
        }

        public void MoveTo(ReadOnlySpan<byte> key)
        {
            _indexIterator.MoveTo(key);

            InitDataIterator();

            _dataIterator?.MoveTo(key);
        }

        public void MoveNext()
        {
            _dataIterator?.MoveNext();

            MoveNextDataIterator();
        }

        private void InitDataIterator()
        {
            if (!_indexIterator.IsValid)
            {
                _dataIterator = null;

                return;
            }

            BlockHandle dataHandle = new(_indexIterator.Value);
            Block? dataBlock = _table.ReadBlock(dataHandle);

            if (dataBlock == null)
            {
                _dataIterator = null;

                return;
            }

            _dataIterator?.Dispose();
            _dataIterator = dataBlock.GetIterator();
        }

        private void MoveNextDataIterator()
        {
            while (_dataIterator != null && !_dataIterator.IsValid)
            {
                _indexIterator.MoveNext();

                InitDataIterator();

                _dataIterator?.MoveToFirst();
            }
        }

        public void Dispose()
        {
            if (!_disposed)
            {
                return;
            }

            _dataIterator?.Dispose();
            _indexIterator.Dispose();

            _disposed = true;
        }
    }
}
