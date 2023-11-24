using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using Evdb.Storage.LogsDb;

namespace Evdb.Storage.LogsDb.Format;

internal interface IBlockCache
{
    void Set(FileId file, BlockHandle handle, Block block);
    bool TryGet(FileId file, BlockHandle handle, [MaybeNullWhen(false)] out Block? block);
}

internal sealed class PinningBlockCache : IBlockCache
{
    private readonly ConcurrentDictionary<BlockCacheHandle, Block> _blocks;

    public PinningBlockCache()
    {
        _blocks = new ConcurrentDictionary<BlockCacheHandle, Block>();
    }

    public void Set(FileId file, BlockHandle handle, Block block)
    {
        _blocks[new(file, handle)] = block;
    }

    public bool TryGet(FileId file, BlockHandle handle, [MaybeNullWhen(false)] out Block? block)
    {
        return _blocks.TryGetValue(new(file, handle), out block);
    }
}

internal sealed class WeakReferenceBlockCache : IBlockCache
{
    private readonly ConcurrentDictionary<BlockCacheHandle, WeakReference<Block>> _blocks;

    public WeakReferenceBlockCache()
    {
        _blocks = new ConcurrentDictionary<BlockCacheHandle, WeakReference<Block>>();
    }

    public void Set(FileId file, BlockHandle handle, Block block)
    {
        _blocks[new(file, handle)] = new WeakReference<Block>(block);
    }

    public bool TryGet(FileId file, BlockHandle handle, [MaybeNullWhen(false)] out Block? block)
    {
        block = default;

        return _blocks.TryGetValue(new(file, handle), out WeakReference<Block>? blockRef) && blockRef.TryGetTarget(out block);
    }
}

internal struct BlockCacheHandle : IEquatable<BlockCacheHandle>
{
    public FileId File;
    public BlockHandle Handle;

    public BlockCacheHandle(FileId file, BlockHandle handle)
    {
        File = file;
        Handle = handle;
    }

    public override bool Equals([NotNullWhen(true)] object? obj)
    {
        return obj is BlockCacheHandle handle && Equals(handle);
    }

    public bool Equals(BlockCacheHandle other)
    {
        return other.File.Equals(File) && other.Handle.Equals(Handle);
    }

    public readonly override int GetHashCode()
    {
        return HashCode.Combine(File, Handle);
    }
}
