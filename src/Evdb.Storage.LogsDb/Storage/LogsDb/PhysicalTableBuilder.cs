using Evdb.Collections;
using Evdb.Storage.LogsDb.Format;
using Evdb.IO;

namespace Evdb.Storage.LogsDb;

internal sealed class PhysicalTableBuilder
{
    private byte[]? _firstKey;
    private byte[]? _lastKey;

    private readonly ulong _dataBlockSize;
    private readonly ulong _bloomBlockSize;
    private readonly BloomFilter _filter;

    private BlockBuilder _data;
    private BlockBuilder _index;

    public Stream BaseStream { get; }

    public PhysicalTableBuilder(Stream stream, ulong dataBlockSize, ulong bloomBlockSize)
    {
        BaseStream = stream;

        _dataBlockSize = dataBlockSize;
        _bloomBlockSize = bloomBlockSize;

        // FIXME: Make this configurable.
        _filter = new BloomFilter(new byte[_bloomBlockSize]);

        _data = new BlockBuilder();
        _index = new BlockBuilder();
    }

    public void Add(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
    {
        // TODO(Perf): We can avoid some allocations here.
        _lastKey = key.ToArray();
        _firstKey ??= _lastKey;

        _data.Add(key, value);
        _filter.Set(key);

        // FIXME: Make this configurable.
        if (_data.Length >= _dataBlockSize)
        {
            BlockHandle dhandle = WriteBlock(ref _data);

            _index.Add(key, BlockHandle.Encode(dhandle));
            _data.Reset();
        }
    }

    public void Complete()
    {
        if (!_data.IsEmpty)
        {
            BlockHandle dhandle = WriteBlock(ref _data);

            _index.Add(_lastKey, BlockHandle.Encode(dhandle));
        }

        BlockHandle ihandle = WriteBlock(ref _index);

        WriteFooter(ihandle);
    }

    private BlockHandle WriteBlock(ref BlockBuilder block)
    {
        block.Complete();
        block.CopyTo(BaseStream);

        return new BlockHandle((ulong)BaseStream.Position - block.Length, block.Length);
    }

    private void WriteFooter(BlockHandle indexHandle)
    {
        BinaryEncoder encoder = new();

        encoder.ByteArray(_filter.Span);
        encoder.ByteArray(_firstKey);
        encoder.ByteArray(_lastKey);
        encoder.ByteArrayRaw(BlockHandle.Encode(indexHandle));
        encoder.UInt32((uint)encoder.Length);

        BaseStream.Write(encoder.Span);
    }
}
