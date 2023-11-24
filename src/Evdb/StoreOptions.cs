using Evdb.Storage;

namespace Evdb;

public class StoreOptions
{
    public string Path { get; set; } = default!;
    public IDatabase Database { get; set; } = default!;
}
