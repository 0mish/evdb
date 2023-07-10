﻿using Evdb.IO;

namespace Evdb.Indexes.Lsm;

public class LsmIndexOptions
{
    public int VirtualTableSize { get; set; } = 1024 * 16;
    public int VirtualTableMaxSize { get; set; } = 1024 * 1024 * 16;

    public string Path { get; set; } = default!;
    public IFileSystem FileSystem { get; set; } = new FileSystem();
}
