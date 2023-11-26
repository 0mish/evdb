using LogsDb.Collections;
using LogsDb.IO;

namespace LogsDb.Formats.Blocked;

internal sealed class BlockedPhysicalTableReader : IPhysicalTableReader
{
    private bool _disposed;

    private FileStream? _file;
    private BloomFilter? _filter;
    private Block? _index;
    private Footer _footer;

    private readonly IFileSystem _fs;
    private readonly IBlockCache _blockCache;

    private FileMetadata Metadata { get; } = default!; // FIXME.

    public BlockedPhysicalTableReader(IFileSystem fs, IBlockCache cache)
    {
        _fs = fs;
        _blockCache = cache;
    }

    public Status Open()
    {
        return Status.Success;
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

    public Iterator GetIterator()
    {
        return new Iterator(this);
    }

    IIterator IPhysicalTableReader.GetIterator()
    {
        return GetIterator();
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
        private readonly BlockedPhysicalTableReader _reader;

        public ReadOnlySpan<byte> Key => _dataIterator!.Key;
        public ReadOnlySpan<byte> Value => _dataIterator!.Value;
        public bool IsValid => !_disposed && _indexIterator.IsValid;

        public Iterator(BlockedPhysicalTableReader reader)
        {
            _reader = reader;
            _indexIterator = reader._index!.GetIterator(); // FIXME: This can throw if we do not open the table.
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
            Block? dataBlock = _reader.ReadBlock(dataHandle);

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
