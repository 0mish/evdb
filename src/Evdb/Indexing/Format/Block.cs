using Evdb.IO;
using System.Text;

namespace Evdb.Indexing.Format;

internal sealed class Block
{
    private readonly byte[] _data;

    public Block(byte[] data)
    {
        _data = data;
    }

    public Iterator GetIterator()
    {
        return new Iterator(_data);
    }

    public sealed class Iterator : IIterator
    {
        private bool _disposed;

        private byte[]? _key;
        private byte[]? _value;

        private readonly BinaryReader _reader;
        private readonly MemoryStream _stream;

        public ReadOnlySpan<byte> Key => _key;
        public ReadOnlySpan<byte> Value => _value;
        public bool IsValid => !_disposed && _key != null && _value != null;

        public Iterator(byte[] data)
        {
            _stream = new MemoryStream(data);
            _reader = new BinaryReader(_stream, Encoding.UTF8, leaveOpen: true);
        }

        public void MoveToFirst()
        {
            _stream.Seek(0, SeekOrigin.Begin);

            MoveNext();
        }

        public void MoveTo(ReadOnlySpan<byte> key)
        {
            if (!IsValid)
            {
                MoveToFirst();
            }
            else
            {
                int cmp = key.SequenceCompareTo(Key);

                if (cmp == 0)
                {
                    return;
                }

                if (cmp < 0)
                {
                    MoveToFirst();
                }
            }

            while (IsValid && key.SequenceCompareTo(Key) > 0)
            {
                MoveNext();
            }
        }

        public void MoveNext()
        {
            if (_stream.Position < _stream.Length)
            {
                _key = _reader.ReadByteArray();
                _value = _reader.ReadByteArray();
            }
            else
            {
                _key = null;
                _value = null;
            }
        }

        public void Dispose()
        {
            if (_disposed)
            {
                return;
            }

            _reader.Dispose();
            _stream.Dispose();

            _disposed = true;
        }
    }
}
