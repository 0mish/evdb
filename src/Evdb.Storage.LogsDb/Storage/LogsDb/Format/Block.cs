using Evdb.Storage.LogsDb;
using Evdb.IO;

namespace Evdb.Storage.LogsDb.Format;

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

        private bool _eob;
        private ArraySegment<byte> _key;
        private ArraySegment<byte> _value;
        private BinaryDecoder _decoder;

        public ReadOnlySpan<byte> Key => _key.AsSpan();
        public ReadOnlySpan<byte> Value => _value.AsSpan();
        public bool IsValid => !_disposed && !_eob;

        public Iterator(byte[] data)
        {
            _decoder = new BinaryDecoder(data);
        }

        public void MoveToFirst()
        {
            _decoder.Reset();

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
            if (!_decoder.IsEmpty)
            {
                _decoder.ByteArray(out _key);
                _decoder.ByteArray(out _value);

                _eob = false;
            }
            else
            {
                _eob = true;
            }
        }

        public void Dispose()
        {
            _disposed = true;
        }
    }
}
