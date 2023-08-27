namespace Evdb;

public class Store
{
    public Store(string path)
    {

    }

    public void Write(string name, in ReadOnlySpan<byte> value)
    {

    }

    public void Read(string name, out ReadOnlySpan<byte> value)
    {
        value = default;
    }
}
