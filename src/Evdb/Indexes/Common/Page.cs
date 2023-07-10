namespace Evdb.Indexes.Common;

public class Page
{
    private readonly byte[] _data;

    public int Number { get; }
    public Span<byte> Span => _data;

    public Page(int number, byte[] data)
    {
        ArgumentNullException.ThrowIfNull(data, nameof(data));

        Number = number;
        _data = data;
    }
}
