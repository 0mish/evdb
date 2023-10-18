using Evdb.Collections;
using Evdb.Indexing.Format;
using Evdb.IO;
using System.Diagnostics;
using System.Text;

namespace Evdb.Indexing;

[DebuggerDisplay("{DebuggerDisplay,nq}")]
internal sealed class PhysicalTable : File, IDisposable
{
    private string DebuggerDisplay => $"PhysicalTable {Metadata.Path}";

    private bool _disposed;

    private Stream _file = default!;
    private BinaryReader _reader = default!;

    private readonly BloomFilter? _filter;
    private readonly Block _index;
    private readonly Footer _footer;

    private readonly IFileSystem _fs;
    private readonly IBlockCache _blockCache;

    public PhysicalTable(IFileSystem fs, FileMetadata metadata, IBlockCache blockCache) : base(metadata)
    {
        _fs = fs;
        _blockCache = blockCache;

        Open();

        _footer = ReadFooter();
        _filter = ReadFilter();
        _index = ReadIndex();
    }

    private void Open()
    {
        _file = _fs.OpenFile(Metadata.Path, FileMode.Open, FileAccess.Read, FileShare.None);
        _reader = new(_file, Encoding.UTF8, leaveOpen: true);
    }

    public bool TryGet(ReadOnlySpan<byte> key, out ReadOnlySpan<byte> value)
    {
        // If not in range of keys in the table, we exit early.
        if (key.SequenceCompareTo(_footer.FirstKey) < 0 || key.SequenceCompareTo(_footer.LastKey) > 0)
        {
            value = default;

            return false;
        }

        // If not in filter, we exit early.
        if (_filter != null && !_filter.Test(key))
        {
            value = default;

            return false;
        }

        // Otherwise we perform the look up in the file.
        using Iterator iter = GetIterator();

        iter.MoveTo(key);

        if (iter.IsValid && iter.Key.SequenceEqual(key))
        {
            value = iter.Value;

            return true;
        }

        value = default;

        return false;
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

        _reader?.Dispose();
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
        _reader.BaseStream.Seek(-sizeof(int), SeekOrigin.End);

        int footerLength = _reader.ReadInt32();

        _reader.BaseStream.Seek(-sizeof(int) - footerLength, SeekOrigin.End);

        return new Footer
        {
            Filter = _reader.ReadByteArray(),
            FirstKey = _reader.ReadByteArray(),
            LastKey = _reader.ReadByteArray(),
            IndexBlock = BlockHandle.Read(_reader)
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
            _indexIterator = table._index.GetIterator();
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
