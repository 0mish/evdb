using Evdb.Collections;
using Evdb.Indexing.Format;
using Evdb.IO;
using System.Text;

namespace Evdb.Indexing;

internal sealed class PhysicalTableBuilder : IDisposable
{
    private bool _disposed;

    private byte[]? _firstKey;
    private byte[]? _lastKey;

    private readonly BloomFilter _filter;

    private BlockBuilder? _data;
    private readonly BlockBuilder _index;

    private readonly BinaryWriter _writer;

    public Stream BaseStream { get; }

    public PhysicalTableBuilder(Stream stream, bool leaveOpen)
    {
        BaseStream = stream;

        _writer = new BinaryWriter(stream, Encoding.UTF8, leaveOpen);
        _filter = new BloomFilter(new byte[4096]);

        _data = null;
        _index = new BlockBuilder(new MemoryStream(), leaveOpen: false);
    }

    public void Add(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
    {
        _lastKey = key.ToArray();
        _firstKey ??= _lastKey;

        _data ??= new BlockBuilder(new MemoryStream(), leaveOpen: false);
        _data.Add(key, value);
        _filter.Set(key);

        // FIXME: Make this configurable.
        if (_data.Length >= 1024 * 16)
        {
            BlockHandle dhandle = WriteBlock(_data);

            _index.Add(key, BlockHandle.Encode(dhandle));
            _data.Dispose();
            _data = null;
        }
    }

    private BlockHandle WriteBlock(BlockBuilder block)
    {
        block.BaseStream.Seek(0, SeekOrigin.Begin);
        block.BaseStream.CopyTo(BaseStream);

        BlockHandle handle = new((ulong)BaseStream.Position - block.Length, block.Length);

        return handle;
    }

    private void WriteFooter(BlockHandle indexHandle)
    {
        long startPos = _writer.BaseStream.Position;

        _writer.WriteByteArray(_filter.Buffer);
        _writer.WriteByteArray(_firstKey);
        _writer.WriteByteArray(_lastKey);
        _writer.Write(BlockHandle.Encode(indexHandle));

        long endPos = _writer.BaseStream.Position;

        _writer.Write((int)(endPos - startPos));
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        if (_data != null)
        {
            BlockHandle dhandle = WriteBlock(_data);

            _index.Add(_lastKey, BlockHandle.Encode(dhandle));
        }

        BlockHandle ihandle = WriteBlock(_index);

        WriteFooter(ihandle);

        _index.Dispose();
        _data?.Dispose();
        _writer.Dispose();

        _disposed = true;
    }
}
