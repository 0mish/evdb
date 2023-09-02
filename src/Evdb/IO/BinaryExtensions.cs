namespace Evdb.IO;

internal static class BinaryExtensions
{
    public static byte[] ReadByteArray(this BinaryReader reader)
    {
        return reader.ReadBytes(reader.Read7BitEncodedInt());
    }

    public static void WriteByteArray(this BinaryWriter writer, ReadOnlySpan<byte> data)
    {
        writer.Write7BitEncodedInt(data.Length);
        writer.Write(data);
    }
}
