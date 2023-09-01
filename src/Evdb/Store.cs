namespace Evdb;

public struct RecordIterator
{

}

public class RecordStream
{
    public void Append(in ReadOnlySpan<byte> value)
    {
        throw new NotImplementedException();
    }

    public RecordIterator Read()
    {
        throw new NotImplementedException();
    }
}

public class Store
{
    public Store(string path)
    {

    }

    public RecordStream All()
    {
        throw new NotImplementedException();
    }

    public RecordStream Get(string name)
    {
        throw new NotImplementedException();
    }

    public bool Exists(string name)
    {
        throw new NotImplementedException();
    }
}
