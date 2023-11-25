using Evdb.IO;
using Evdb.Storage.LogsDb;
using Evdb.Storage.LogsDb.Format;

namespace Evdb.Tests.Storage;

public class ManifestTests
{
    private const string TestPath = "manifest-test";

    private FileSystem _fs;
    private IBlockCache _blockCache;
    private Manifest _manifest;

    [SetUp]
    public void SetUp()
    {
        _fs = new FileSystem();
        _blockCache = new WeakReferenceBlockCache();
        _manifest = new Manifest(_fs, TestPath, _blockCache, manifestLogSize: 1024);
    }

    [TearDown]
    public void TearDown()
    {
        _manifest.Dispose();

        Directory.Delete(TestPath, recursive: true);
    }

    [Test]
    public void Ctor__Empty_State()
    {
        Assert.Multiple(() =>
        {
            Assert.That(_manifest.Path, Is.EqualTo(TestPath));
            Assert.That(_manifest.Current, Is.Not.Null);

            ManifestState state = _manifest.Current;

            Assert.That(state.VirtualTables, Is.Empty);
            Assert.That(state.PhysicalTables, Is.Empty);
            Assert.That(state.PhysicalLogs, Is.Empty);
        });
    }

    [Test]
    public void Commit__Edit_VirtualTable__Edited()
    {
        // Arrange
        _manifest.Open();

        VirtualTable table = new(log: null, capacity: 1024);
        ManifestEdit edit0 = new(
            vtables: new ListEdit<VirtualTable>(
                registered: new[] { table }
            )
        );
        ManifestEdit edit1 = new(
            vtables: new ListEdit<VirtualTable>(
                unregistered: new[] { table }
            )
        );

        // Act
        ManifestState s0 = _manifest.Current;
        _manifest.Commit(edit0);

        ManifestState s1 = _manifest.Current;
        _manifest.Commit(edit1);

        ManifestState s2 = _manifest.Current;

        // Assert
        Assert.Multiple(() =>
        {
            Assert.That(s0.VirtualTables, Is.Empty);
            Assert.That(s1.VirtualTables, Is.EquivalentTo(new[] { table }));
            Assert.That(s2.VirtualTables, Is.Empty);
        });
    }

    [Test]
    public void Commit__Edit_PhysicalLog__Edited()
    {
        // Arrange
        _manifest.Open();

        PhysicalLog log = new(_fs, new FileMetadata(TestPath, FileType.Log, 1));
        ManifestEdit edit0 = new(
            plogs: new ListEdit<PhysicalLog>(
                registered: new[] { log }
            )
        );
        ManifestEdit edit1 = new(
            plogs: new ListEdit<PhysicalLog>(
                unregistered: new[] { log }
            )
        );

        // Act
        ManifestState s0 = _manifest.Current;
        _manifest.Commit(edit0);

        ManifestState s1 = _manifest.Current;
        _manifest.Commit(edit1);

        ManifestState s2 = _manifest.Current;

        // Assert
        Assert.Multiple(() =>
        {
            Assert.That(s0.PhysicalLogs, Is.Empty);
            Assert.That(s1.PhysicalLogs, Is.EquivalentTo(new[] { log }));
            Assert.That(s2.PhysicalLogs, Is.Empty);
        });
    }

    [Test]
    public void Commit__Edit_PhysicalTable__Edited()
    {
        // Arrange
        _manifest.Open();

        PhysicalTable table = new(_fs, new FileMetadata(TestPath, FileType.Table, 1), new WeakReferenceBlockCache());
        ManifestEdit edit0 = new(
            ptables: new ListEdit<PhysicalTable>(
                registered: new[] { table }
            )
        );
        ManifestEdit edit1 = new(
            ptables: new ListEdit<PhysicalTable>(
                unregistered: new[] { table }
            )
        );

        // Act
        ManifestState s0 = _manifest.Current;
        _manifest.Commit(edit0);

        ManifestState s1 = _manifest.Current;
        _manifest.Commit(edit1);

        ManifestState s2 = _manifest.Current;

        // Assert
        Assert.Multiple(() =>
        {
            Assert.That(s0.PhysicalTables, Is.Empty);
            Assert.That(s1.PhysicalTables, Is.EquivalentTo(new[] { table }));
            Assert.That(s2.PhysicalTables, Is.Empty);
        });
    }

    [Test]
    public void Open__No_Manifest__Empty_State()
    {
        // Act
        _manifest.Open();

        // Assert
        ManifestState state = _manifest.Current;

        Assert.Multiple(() =>
        {
            Assert.That(state.VirtualTables, Is.Empty);
            Assert.That(state.PhysicalTables, Is.Empty);
            Assert.That(state.PhysicalLogs, Is.Empty);
        });
    }

    [Test]
    public void Open__Manifest_Exists__Recovered_State()
    {
        // Arrange
        _manifest.Open();

        PhysicalLog log = new(_fs, new FileMetadata(TestPath, FileType.Log, 1));
        ManifestEdit edit = new(
            plogs: new ListEdit<PhysicalLog>(
                registered: new[] { log }
            )
        );

        _manifest.Commit(edit);
        _manifest.Dispose();

        _manifest = new Manifest(_fs, TestPath, _blockCache, manifestLogSize: 1024);

        // Act
        Status status = _manifest.Open();

        // Assert
        ManifestState state = _manifest.Current;

        Assert.That(status.Code, Is.EqualTo(StatusCode.Success));
        Assert.Multiple(() =>
        {
            Assert.That(state.VirtualTables, Is.Empty);
            Assert.That(state.PhysicalTables, Is.Empty);
            Assert.That(state.PhysicalLogs, Has.Length.EqualTo(1));
            Assert.That(state.PhysicalLogs[0].Metadata.Id, Is.EqualTo(log.Metadata.Id));
        });
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
}
