namespace Evdb.Benchmarks.IO;

public class DirectoryUtils
{
    public static void WaitNoModification(string path)
    {
        bool changed = true;
        Dictionary<string, DateTime> modified = new();

        while (changed)
        {
            changed = false;

            foreach (string file in Directory.GetFiles(path))
            {
                DateTime newModifiedWhen = File.GetLastWriteTime(file);

                if (!modified.TryGetValue(file, out DateTime modifiedWhen) || newModifiedWhen != modifiedWhen)
                {
                    modified[file] = newModifiedWhen;

                    changed = true;
                }
            }

            if (changed)
            {
                Thread.Sleep(1000);
            }
        }
    }
}
