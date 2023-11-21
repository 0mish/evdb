using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Evdb.Memory;

internal unsafe sealed class Arena : IDisposable
{
    private bool _disposed;
    private Block _blocks;
    private Block? _extraBlocks;

    private readonly nuint _blockSize;

    public Arena(nuint blockSize = 1024 * 32)
    {
        _blocks = AllocateBlock(blockSize, ref _blocks!);
        _extraBlocks = null;

        _blockSize = blockSize;
    }

    public T* Allocate<T>(int count = 1, nuint alignment = 1) where T : unmanaged
    {
        return (T*)Allocate((nuint)count * (nuint)sizeof(T), alignment);
    }

    // TODO: Make this concurrent.
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void* Allocate(nuint size, nuint alignment = 1)
    {
        if (size <= _blockSize)
        {
            Block block = _blocks;

            byte* pointer = block.Pointer;
            byte* alignedPointer = (byte*)((nuint)(pointer + (alignment - 1)) & ~(alignment - 1));
            byte* endPointer = alignedPointer + size;

            if (endPointer <= block.EndPointer)
            {
                block.Pointer = endPointer;

                return alignedPointer;
            }

            return AllocateSlow(size);
        }

        return AllocateBlockSlow(size);

        // NOTE:
        //
        // Manually perform method outlining and help the JIT turn these into tail-calls eliminating the call frame
        // setup.
        [MethodImpl(MethodImplOptions.NoInlining)]
        void* AllocateSlow(nuint size)
        {
            Block block = AllocateBlock(_blockSize, ref _blocks!);
            void* pointer = block.Pointer;

            block.Pointer += size;

            return pointer;
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        void* AllocateBlockSlow(nuint size)
        {
            return AllocateBlock(size, ref _extraBlocks).StartPointer;
        }
    }

    private static Block AllocateBlock(nuint size, ref Block? tail)
    {
        byte* data = (byte*)NativeMemory.Alloc(size);
        Block block = new()
        {
            Pointer = data,
            StartPointer = data,
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

        FreeList(_blocks);
        FreeList(_extraBlocks);

        _disposed = true;

        static void FreeList(Block? tail)
        {
            Block? node = tail;

            while (node != null)
            {
                NativeMemory.Free(node.StartPointer);

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
        public byte* StartPointer;
        public byte* Pointer;
        public byte* EndPointer;
        public Block? Previous;
    }
}
