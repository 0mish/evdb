using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Evdb.IO;

internal struct BinaryEncoder
{
    private int _length;
    private byte[] _buffer;

    public readonly ReadOnlySpan<byte> Span => new(_buffer, 0, _length);
    public readonly byte[] Buffer => _buffer;
    public readonly ulong Length => (ulong)_length;
    public readonly bool IsEmpty => _length == 0;

    public BinaryEncoder(byte[] buffer)
    {
        Debug.Assert(buffer != null);

        _buffer = buffer;
    }

    public void UInt32(uint value)
    {
        EnsureCapacity(4);

        ref byte @base = ref MemoryMarshal.GetArrayDataReference(_buffer);
        ref byte ptr = ref Unsafe.Add(ref @base, _length);

        // FIXME: Endianness.
        Unsafe.As<byte, uint>(ref ptr) = value;

        _length += 4;
    }

    public void VarUInt32(uint value)
    {
        EnsureCapacity(8);
        EncodeVarInt(value);
    }

    public void VarInt32(int value)
    {
        VarUInt32((uint)value);
    }

    public void VarUInt64(ulong value)
    {
        EnsureCapacity(16);
        EncodeVarInt(value);
    }

    public void VarInt64(long value)
    {
        VarUInt64((ulong)value);
    }

    public void ByteArray(ReadOnlySpan<byte> value)
    {
        EnsureCapacity(8 + value.Length);
        EncodeVarInt((uint)value.Length);

        value.CopyTo(_buffer.AsSpan().Slice(_length));

        _length += value.Length;
    }

    public void ByteArrayRaw(ReadOnlySpan<byte> value)
    {
        EnsureCapacity(value.Length);

        value.CopyTo(_buffer.AsSpan().Slice(_length));

        _length += value.Length;
    }

    public void Reset()
    {
        _length = 0;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void EnsureCapacity(int space)
    {
        Debug.Assert(_buffer != null);

        if (_buffer.Length < _length + space)
        {
            ExpandCapacity(space);
        }
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private void ExpandCapacity(int space)
    {
        int newCapacity = int.Max(int.Max(_buffer.Length + space, 256), _buffer.Length * 2);

        Array.Resize(ref _buffer, newCapacity);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void EncodeVarInt(ulong value)
    {
        ref byte @base = ref MemoryMarshal.GetArrayDataReference(_buffer);
        ref byte ptr = ref Unsafe.Add(ref @base, _length);

        while (value > 0x7Fu)
        {
            ptr = (byte)(value | ~0x7Fu);
            ptr = ref Unsafe.Add(ref ptr, 1);
            value >>= 7;
        }

        ptr = (byte)value;
        _length = (int)Unsafe.ByteOffset(ref @base, ref ptr) + 1;
    }
}
