using System.Diagnostics;

namespace Evdb.IO;

internal sealed class AppendFile : IDisposable
{
    private bool _disposed;

    private int _offset;
    private int _writers;

    private readonly byte[] _buffer;
    private readonly int _bufferSize;

    private readonly Stream _stream;
    private readonly ManualResetEvent _flush;

    public AppendFile(Stream stream, int bufferSize)
    {
        _flush = new ManualResetEvent(initialState: false);

        _stream = stream;
        _bufferSize = bufferSize;
        _buffer = new byte[bufferSize];

        _offset = 0;
        _writers = 0;
    }

    public void Write(ReadOnlySpan<byte> data, bool forceFlush = false, bool waitFlush = false)
    {
        if (data.Length >= _bufferSize)
        {
            // FIXME: Concurrency.
            _stream.Write(data);
            _stream.Flush();

            return;
        }

        Span<byte> block;

        while (!TryAllocate(data.Length, out block))
        {
            Thread.Yield();
        }

        Interlocked.Increment(ref _writers);

        data.CopyTo(block);

        Interlocked.Decrement(ref _writers);

        if (forceFlush)
        {
            Flush();
        }
        else if (waitFlush)
        {
            _flush.WaitOne();
        }
    }

    private void Flush()
    {

    }

    private bool TryAllocate(int size, out Span<byte> block)
    {
        Debug.Assert(size < _bufferSize);

        block = default;

        if (size == 0)
        {
            return true;
        }

        // If offset is outside the page, it means that we have overflowed. Let the overflow thread resolve the issue.
        if (Volatile.Read(ref _offset) > _bufferSize)
        {
            return false;
        }

        int endOffset = Interlocked.Add(ref _offset, size);
        int startOffset = endOffset - size;

        // If start of block is after the page, this means another thread already overflew. Let the overflow thread
        // resolve the issue.
        if (startOffset > _bufferSize)
        {
            return false;
        }

        // If start of block did not overflow and end of block overflew, this means we are the overflow thread.
        if (endOffset > _bufferSize)
        {
            // If there are writers still writing to the buffer, we wait for them to complete. No more writers will
            // enter since we overflew, so we can wait safely.
            while (_writers > 0)
            {
                Thread.Yield();
            }

            _stream.Write(_buffer);
            _stream.Flush();

            startOffset = 0;

            Volatile.Write(ref _offset, size);
        }

        block = _buffer.AsSpan().Slice(startOffset, size);

        return true;
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _stream.Write(_buffer, 0, _offset);
        _stream.Dispose();
        _flush.Dispose();

        _disposed = true;
    }
}
