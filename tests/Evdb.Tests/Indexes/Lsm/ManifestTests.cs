﻿using Evdb.Indexes.Lsm;
using Evdb.IO;

namespace Evdb.Tests.Indexes.Lsm;

public class ManifestTests
{
    private FileSystem _fs;
    private Manifest _manifest;

    [SetUp]
    public void SetUp()
    {
        _fs = new FileSystem();
        _manifest = new Manifest(_fs, "manifest-test");
    }

    [TearDown]
    public void TearDown()
    {
        _manifest.Dispose();
    }

    [Test]
    public void Ctor()
    {
        Assert.Multiple(() =>
        {
            Assert.That(_manifest.Path, Is.EqualTo("manifest-test"));
            Assert.That(_manifest.Current, Is.Not.Null);
        });
    }

    [Test]
    public void Ctor__ArgumentNull()
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => new Manifest(null!, ""), Throws.ArgumentNullException);
            Assert.That(() => new Manifest(new FileSystem(), null!), Throws.ArgumentNullException);
        });
    }

    [Test]
    public void NextVersionNumber__ReturnsValue()
    {
        // Arrange
        ulong versionNo = _manifest.VersionNumber;

        // Act
        _manifest.NextVersionNumber();

        // Assert
        Assert.That(_manifest.VersionNumber, Is.GreaterThan(versionNo));
    }

    [Test]
    public void NextFileNumber__ReturnsValue()
    {
        // Arrange
        ulong fileNo = _manifest.FileNumber;

        // Act
        _manifest.NextFileNumber();

        // Assert
        Assert.That(_manifest.FileNumber, Is.GreaterThan(fileNo));
    }

    [Test]
    public void Commit__Files_Added_Removed()
    {
        // Arrange
        ManifestEdit edit0 = new()
        {
            FilesRegistered = new[]
            {
                new FileId(FileType.Table, 0),
                new FileId(FileType.Table, 1),
                new FileId(FileType.Table, 2)
            }
        };

        ManifestEdit edit1 = new()
        {
            FilesUnregistered = new[]
            {
                new FileId(FileType.Table, 0)
            }
        };

        // Act
        _manifest.Commit(edit0);
        _manifest.Commit(edit1);

        // Assert
        ManifestState state = _manifest.Current;

        Assert.That(state.Files, Is.EquivalentTo(new FileId[]
        {
            new FileId(FileType.Table, 1),
            new FileId(FileType.Table, 2)
        }));
    }

    [Test]
    [Ignore("This is currently broken because Resolve is not implemented.")]
    public void Clean__Dead_Files_Deleted()
    {
        // Arrange
        ManifestEdit edit0 = new()
        {
            FilesRegistered = new[]
            {
                new FileId(FileType.Table, 0),
                new FileId(FileType.Table, 1),
                new FileId(FileType.Table, 2)
            }
        };

        ManifestEdit edit1 = new()
        {
            FilesUnregistered = new[]
            {
                new FileId(FileType.Table, 0)
            }
        };

        // Act
        _manifest.Commit(edit0);
        _manifest.Commit(edit1);
        _manifest.Clean();

        // Assert
        // TODO: Assert that files were deleted.
    }

    [Test]
    [Ignore("Resolve is not implemented.")]
    public void Resolve__()
    {

    }
}
