using Evdb.IO;
using System.Text;

namespace Evdb.Tests.IO;

public class MemoryFileSystem : IFileSystem
{
    private readonly DirectoryNode _root;

    public MemoryFileSystem()
    {
        _root = new DirectoryNode();
    }

    public void CreateDirectory(string path)
    {

    }

    public void DeleteFile(string path)
    {
        Node? node = GetNode(GetSegments(path), out DirectoryNode closestNode);

        if (node is not null)
        {
            closestNode.Children.Remove(node);
        }
    }

    public string[] ListFiles(string path)
    {
        Node node = GetNode(GetSegments(path), out _) ?? throw new DirectoryNotFoundException();

        if (node is not DirectoryNode dirNode)
        {
            throw new IOException();
        }

        return dirNode.Children.Select(f => f.Path).ToArray();
    }

    public Stream OpenFile(string path, FileMode mode, FileAccess access)
    {
        Node? node = GetNode(GetSegments(path), out DirectoryNode? closestNode);

        if (node is null)
        {
            if (mode is FileMode.Open or FileMode.Truncate or FileMode.Append)
            {
                throw new FileNotFoundException();
            }

            node = new FileNode
            {
                Name = Path.GetFileName(path)
            };

            closestNode.Children.Add(node);
        }

        if (node is not FileNode fileNode)
        {
            throw new IOException();
        }

        return fileNode.Data;
    }

    private Node? GetNode(string[] segments, out DirectoryNode closestNode)
    {
        closestNode = _root;

        if (segments.Length == 0)
        {
            return null;
        }

        Node? node = _root;

        foreach (string segment in segments)
        {
            if (node is null)
            {
                break;
            }
            else if (node is DirectoryNode dirNode)
            {
                node = dirNode.Children.SingleOrDefault(n => n.Name == segment);
                closestNode = dirNode;
            }
        }

        return node;
    }

    private static string[] GetSegments(string path)
    {
        return path.Split(new char[] { Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar }, StringSplitOptions.RemoveEmptyEntries);
    }

    private class Node
    {
        public Node? Parent { get; set; }
        public string? Name { get; set; }

        public string Path
        {
            get
            {
                Node? node = this;
                List<string?> segments = new();
                StringBuilder builder = new();

                while (node != null)
                {
                    segments.Add(node.Name);
                    node = node.Parent;
                }

                for (int i = segments.Count - 1; i >= 0; i--)
                {
                    builder.Append(System.IO.Path.DirectorySeparatorChar).Append(segments[i]);
                }

                return builder.ToString();
            }
        }
    }

    private class DirectoryNode : Node
    {
        public List<Node> Children { get; } = new();
    }

    private class FileNode : Node
    {
        public MemoryStream Data { get; } = new();
    }
}
