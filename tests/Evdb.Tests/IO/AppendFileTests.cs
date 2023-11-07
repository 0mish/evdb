using Evdb.IO;
using System.Buffers.Binary;
using System.Collections;

namespace Evdb.Tests.IO;

public class AppendFileTests
{
    [Test]
    public void Write__Multiple__BytesAppended()
    {
        const int Count = 1024;
        const int AllocationSize = 16;

        // Arrange
        Span<byte> data = stackalloc byte[AllocationSize];

        using (FileStream stream = File.Open($"{TestContext.CurrentContext.Test.Name}.ulog", FileMode.Create))
        using (AppendFile writer = new(stream, bufferSize: 4096))
        {
            // Act
            for (int i = 0; i < Count; i++)
            {
                data.Clear();

                BinaryPrimitives.WriteInt32LittleEndian(data, i);

                writer.Write(data);

                Assert.That(data.Length, Is.EqualTo(AllocationSize));
            }
        }

        // Assert
        using FileStream wstream = File.Open($"{TestContext.CurrentContext.Test.Name}.ulog", FileMode.Open);

        for (int i = 0; i < Count; i++)
        {
            int read = wstream.Read(data);

            Assert.That(read, Is.EqualTo(AllocationSize));

            int value = BinaryPrimitives.ReadInt32LittleEndian(data);

            Assert.That(value, Is.EqualTo(i));
        }
    }

    [Test]
    public void Write__Concurrent_Multiple__BytesAppended()
    {
        const int Count = 1024;
        const int AllocationSize = 16;

        // Arrange
        using (ManualResetEventSlim @event = new(initialState: false))
        using (FileStream stream = File.Open($"{TestContext.CurrentContext.Test.Name}.ulog", FileMode.Create))
        using (AppendFile writer = new(stream, bufferSize: 4096))
        {
            // Act
            Task[] tasks = new Task[Environment.ProcessorCount];

            for (int i = 0; i < tasks.Length; i++)
            {
                int thread = i;

                tasks[i] = Task.Run(() =>
                {
                    @event.Wait();

                    for (int j = 0; j < Count; j++)
                    {
                        Span<byte> data = new byte[AllocationSize];

                        BinaryPrimitives.WriteInt32LittleEndian(data, j + Count * thread);

                        writer.Write(data);

                        Assert.That(data.Length, Is.EqualTo(AllocationSize));
                    }
                });
            }

            @event.Set();

            Task.WaitAll(tasks);
        }

        // Assert
        using FileStream wstream = File.Open($"{TestContext.CurrentContext.Test.Name}.ulog", FileMode.Open);

        BitArray set = new(Count * Environment.ProcessorCount);

        for (int i = 0; i < Count * Environment.ProcessorCount; i++)
        {
            Span<byte> data = new byte[AllocationSize];
            int read = wstream.Read(data);

            Assert.That(read, Is.EqualTo(AllocationSize));

            int value = BinaryPrimitives.ReadInt32LittleEndian(data);

            Assert.That(set[value], Is.False);

            set.Set(value, true);
        }

        Assert.That(set, Is.All.EqualTo(true));
    }
}
