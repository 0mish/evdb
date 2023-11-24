namespace Evdb.IO;

internal static class BinaryExtensions
{
    public static byte[] ReadByteArray(this BinaryReader reader)
    {
        return reader.ReadBytes(reader.Read7BitEncodedInt());
    }
}
