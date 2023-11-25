using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Evdb.IO;

internal struct BinaryDecoder
{
    private int _position;
    private readonly byte[] _buffer;

    public readonly ulong Position => (ulong)_position;
    public readonly bool IsEmpty => _position >= _buffer.Length;

    public BinaryDecoder(byte[] buffer)
    {
        _buffer = buffer ?? Array.Empty<byte>();
    }

    public bool UInt32(out uint value)
    {
        if (_position + 4 <= _buffer.Length)
        {
            ref byte @base = ref MemoryMarshal.GetArrayDataReference(_buffer);
            ref byte ptr = ref Unsafe.Add(ref @base, _position);

            // FIXME: Endianness.
            value = Unsafe.As<byte, uint>(ref ptr);

            _position += 4;

            return true;
        }

        value = 0;

        return false;
    }

    public bool UInt64(out ulong value)
    {
        if (_position + 8 <= _buffer.Length)
        {
            ref byte @base = ref MemoryMarshal.GetArrayDataReference(_buffer);
            ref byte ptr = ref Unsafe.Add(ref @base, _position);

            // FIXME: Endianness.
            value = Unsafe.As<byte, ulong>(ref ptr);

            _position += 8;

            return true;
        }

        value = 0;

        return false;
    }

    public bool VarUInt32(out uint value)
    {
        value = 0;

        ref byte @base = ref MemoryMarshal.GetArrayDataReference(_buffer);
        ref byte ptr = ref Unsafe.Add(ref @base, _position);
        ref byte end = ref Unsafe.Add(ref @base, _buffer.Length);

        for (int shift = 0; shift <= 28 && Unsafe.IsAddressLessThan(ref ptr, ref end); shift += 7)
        {
            byte curr = ptr;

            ptr = ref Unsafe.Add(ref ptr, 1);
            value |= (curr & 0x7Fu) << shift;

            if (curr <= 0x7Fu)
            {
                _position = (int)Unsafe.ByteOffset(ref @base, ref ptr);

                return true;
            }
        }

        _position = (int)Unsafe.ByteOffset(ref @base, ref ptr);

        return false;
    }

    public bool VarUInt64(out ulong value)
    {
        value = 0;

        ref byte @base = ref MemoryMarshal.GetArrayDataReference(_buffer);
        ref byte ptr = ref Unsafe.Add(ref @base, _position);
        ref byte end = ref Unsafe.Add(ref @base, _buffer.Length);

        for (int shift = 0; shift <= 63 && Unsafe.IsAddressLessThan(ref ptr, ref end); shift += 7)
        {
            byte curr = ptr;

            ptr = ref Unsafe.Add(ref ptr, 1);
            value |= (ulong)(curr & 0x7Fu) << shift;

            if (curr <= 0x7Fu)
            {
                _position = (int)Unsafe.ByteOffset(ref @base, ref ptr);

                return true;
            }
        }

        _position = (int)Unsafe.ByteOffset(ref @base, ref ptr);

        return false;
    }

    public bool ByteArray(out ArraySegment<byte> value)
    {
        if (VarUInt32(out uint length) && _position + length <= _buffer.Length)
        {
            value = new ArraySegment<byte>(_buffer, _position, (int)length);

            _position += (int)length;

            return true;
        }

        value = default;

        return false;
    }

    public void Reset()
    {
        _position = 0;
    }
}
