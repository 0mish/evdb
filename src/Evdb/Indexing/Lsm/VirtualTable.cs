using System.Diagnostics;
using System.Text;
using Evdb.IO;

namespace Evdb.Indexing.Lsm;

[DebuggerDisplay("{DebuggerDisplay,nq}")]
internal sealed class VirtualTable : File, IDisposable
{
    private string DebuggerDisplay => $"VirtualTable {Metadata.Path}";

    private bool _disposed;

    private IndexKey? _minKey;
    private IndexKey? _maxKey;

    private readonly SortedDictionary<IndexKey, byte[]> _kvs;
    private readonly WriteAheadLog _wal;
    private readonly IFileSystem _fs;

    public long Size { get; private set; }
    public long MaxSize { get; }

    public VirtualTable(IFileSystem fs, FileMetadata metadata, long maxSize) : base(metadata)
    {
        ArgumentNullException.ThrowIfNull(fs, nameof(fs));

        MaxSize = maxSize;

        _kvs = new SortedDictionary<IndexKey, byte[]>();
        _wal = new WriteAheadLog(fs, metadata.Path);
        _fs = fs;
    }

    // TODO: Handle case where the record itself is larger than the table size.
    public bool TrySet(IndexKey ikey, in ReadOnlySpan<byte> value)
    {
        long newSize = Size + ikey.Value.Length + value.Length;

        if (newSize > MaxSize)
        {
            return false;
        }

        _wal.LogSet(ikey, value);

        if (Nullable.Compare(ikey, _minKey) < 0)
        {
            _minKey = ikey;
        }

        if (Nullable.Compare(ikey, _maxKey) > 0)
        {
            _maxKey = ikey;
        }

        _kvs[ikey] = value.ToArray();

        Size = newSize;

        return true;
    }

    public bool TryGet(IndexKey ikey, out ReadOnlySpan<byte> value)
    {
        if (Nullable.Compare(ikey, _minKey) < 0 || Nullable.Compare(ikey, _maxKey) > 0)
        {
            value = default;

            return false;
        }

        bool result = _kvs.TryGetValue(ikey, out byte[]? valueCopy);

        value = valueCopy;

        return result;
    }

    // TODO: Consider empty tables.
    public FileMetadata Flush(string path)
    {
        FileMetadata metadata = new(path, FileType.Table, Metadata.Id.Number);

        using (Stream file = _fs.OpenFile(metadata.Path, FileMode.Create, FileAccess.Write))
        using (BinaryWriter writer = new(file, Encoding.UTF8, leaveOpen: true))
        {
            BloomFilter filter = new(size: 4096);

            foreach (KeyValuePair<IndexKey, byte[]> kv in _kvs)
            {
                filter.Set(kv.Key.Value);
            }

            writer.Write7BitEncodedInt(filter.Buffer.Length);
            writer.Write(filter.Buffer);

            writer.Write(_minKey?.Value ?? "");
            writer.Write(_maxKey?.Value ?? "");

            foreach (KeyValuePair<IndexKey, byte[]> kv in _kvs)
            {
                writer.Write(kv.Key.Value);
                writer.Write7BitEncodedInt(kv.Value.Length);
                writer.Write(kv.Value);
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
}
