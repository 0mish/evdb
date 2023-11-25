namespace LogsDb.Tests.Indexing;

internal class FileIdTests
{
    [TestCase("", FileType.Manifest, 1ul, "000001.manifest")]
    [TestCase("test", FileType.Manifest, 1ul, "test\\000001.manifest")]
    [TestCase("test", FileType.Table, 1ul, "test\\000001.olog")]
    [TestCase("test", FileType.Log, 1ul, "test\\000001.ulog")]
    [Platform("Win")]
    public void GetPath__Windows__ReturnsResult(string path, FileType type, ulong number, string expectation)
    {
        // Arrange
        FileId fileId = new(type, number);

        // Act
        string result = fileId.GetPath(path);

        // Assert
        Assert.That(result, Is.EqualTo(expectation));
    }

    [TestCase("00")]
    [TestCase(".ulog")]
    [TestCase("test")]
    [TestCase("dir/test")]
    [TestCase("test.something")]
    [TestCase("dir/00")]
    [TestCase("dir/test.something")]
    [TestCase("dir/00.other")]
    public void TryParse__Invalid__ReturnsFalse(string value)
    {
        // Act
        bool result = FileId.TryParse(value, out _);

        // Assert
        Assert.That(result, Is.False);
    }

    [TestCase("00.ulog", FileType.Log, 0ul)]
    [TestCase("25.olog", FileType.Table, 25ul)]
    [TestCase("32.manifest", FileType.Manifest, 32ul)]
    public void TryParse__Valid__ReturnsTrue(string value, FileType type, ulong number)
    {
        // Act
        bool result = FileId.TryParse(value, out FileId fileId);

        // Assert
        Assert.That(result, Is.True);
        Assert.Multiple(() =>
        {
            Assert.That(fileId.Type, Is.EqualTo(type));
            Assert.That(fileId.Number, Is.EqualTo(number));
        });
    }
}
