namespace Evdb.Indexing;

internal struct ManifestEdit
{
    public object[]? Registered { get; set; }
    public object[]? Unregistered { get; set; }
}
