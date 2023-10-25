using System.Runtime.InteropServices;

namespace Evdb.Memory;

internal unsafe sealed class Arena : IDisposable
{
    private const int BlockSize = 1024 * 16;

    private bool _disposed;
    private Block _tail;
    private Block? _etail;

    public Arena()
    {
        _tail = AllocateBlock(BlockSize, ref _tail!);
        _etail = null;
    }

    public void* Allocate(nuint size, nuint alignment)
    {
        if (size > BlockSize)
        {
            return AllocateBlock(size, ref _etail).Pointer;
        }

        // TODO: Make this concurrent.
        Block block = _tail;
        nuint alignedSize = size + alignment;

        if (block.AllocPointer + alignedSize > block.EndPointer)
        {
            block = AllocateBlock(BlockSize, ref _tail!);
        }

        void* result = block.AllocPointer;
        void* alignedResult = (void*)(((nuint)result + (alignment - 1)) & ~(alignment - 1));

        block.AllocPointer += alignedSize;

        return alignedResult;
    }

    public T* Allocate<T>(int count = 1, nuint alignment = 16) where T : unmanaged
    {
        return (T*)Allocate((nuint)count * (nuint)sizeof(T), alignment);
    }

    private static Block AllocateBlock(nuint size, ref Block? tail)
    {
        byte* data = (byte*)NativeMemory.Alloc(size);
        Block block = new()
        {
            AllocPointer = data,
            Pointer = data,
            EndPointer = data + size,
            Previous = tail
        };

        while (Interlocked.CompareExchange(ref tail, block, block.Previous) != block.Previous)
        {
            block.Previous = tail;
        }

        return block;
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        GC.SuppressFinalize(this);

        FreeList(_tail);
        FreeList(_etail);

        _disposed = true;

        static void FreeList(Block? tail)
        {
            Block? node = tail;

            while (node != null)
            {
                NativeMemory.Free(node.Pointer);

                node = node.Previous;
            }
        }
    }

    ~Arena()
    {
        Dispose();
    }

    private class Block
    {
        public byte* Pointer;
        public byte* AllocPointer;
        public byte* EndPointer;
        public Block? Previous;
    }
}
