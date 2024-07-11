using CommandLine;
using System.Diagnostics;

namespace ArchiveFilemover
{
    public class ArchiveFilemoverMain
    {
        public static async Task<int> Main(string[] args)
        {
            var options = new CommandOptions();
            var result = Parser.Default.ParseArguments<CommandOptions>(args)
                .WithParsed(o => options = o);
            if (result.Errors.Any())
                Environment.Exit(1);

            var sw = Stopwatch.StartNew();
            var items = MoveFiles(options);
            sw.Stop();

            Console.WriteLine($"Move done {items} items traversed in {sw.Elapsed}");

            sw = Stopwatch.StartNew();
            Console.WriteLine("Removing empty dirs...");
            await DeleteEmptyDirs(new(options.SourcePath));
            sw.Stop();
            Console.WriteLine($"Empty removal done after {sw.Elapsed}");

            if (Debugger.IsAttached) Console.ReadKey();
            return 0;
        }

        public static async Task<bool> DeleteEmptyDirs(DirectoryInfo dir)
        {
            // walk all subdirs, and recursively remove
            var tasks = dir.EnumerateDirectories().Select(DeleteEmptyDirs).ToList();

            bool noRemainingDirs = true;
            foreach (var dt in tasks)
            {
                noRemainingDirs = await dt & noRemainingDirs;
            }
            // if subdir failed removal, or there is any files in the dir return as not deleted
            if (!noRemainingDirs || dir.EnumerateFileSystemInfos().Any())
                return false;

            try
            {
                // try to delete, and if ok return true for delete ok
                dir.Delete();
                Console.WriteLine(dir.FullName);
                return true;
            }
            catch (UnauthorizedAccessException) { }
            catch (DirectoryNotFoundException) { }
            // failure to delete is false
            return false;
        }

        private static ParallelQuery<FileInfo> EnumerateFilesParallel(DirectoryInfo dir) =>
            dir.EnumerateDirectories()
                .SelectMany(EnumerateFilesParallel)
                .Concat(dir.EnumerateFiles("*", SearchOption.TopDirectoryOnly))
                .AsParallel();

        private static readonly HashSet<string> HasCreatedDirectories = [];
        private static readonly object HasCreatedDirectoriesLock = new();

        private static long MoveFiles(CommandOptions options)
        {
            long totalItemsTraversed = 0;
            int itemsSincePrint = 0;
            var runStart = DateTime.UtcNow;
            var runStartString = runStart.ToString("yyyyMMdd_HHmm");
            var hasMaxWriteTimeOption = options.MaxWriteTime != DateTime.MinValue;
            var dir = new DirectoryInfo(options.SourcePath);
            EnumerateFilesParallel(dir).ForAll(fi =>
            {
                Interlocked.Increment(ref totalItemsTraversed);
                if (Interlocked.Increment(ref itemsSincePrint) % 10000 == 1)
                {
                    Console.Write('.');
                }

                try
                {
                    var lastWriteUtc = fi.LastWriteTimeUtc;
                    if (hasMaxWriteTimeOption && lastWriteUtc > options.MaxWriteTime)
                        return;

                    var destName = fi.FullName.Substring(options.SourcePath.Length + 1);
                    var destFileName = Path.Combine(options.DestinationPath, $"{runStartString}_modyear_{lastWriteUtc.Year}", destName);
                    var destdir = Path.GetDirectoryName(destFileName);

                    var printDest = destdir.Substring(options.DestinationPath.Length + 1);
                    Console.WriteLine($"{DateTime.UtcNow:yyyy-MM-dd HH:mm:ss.fff} {destName} => {printDest}");
                    itemsSincePrint = 0;

                    if (!HasCreatedDirectories.Contains(destdir))
                    {
                        Directory.CreateDirectory(destdir);
                        lock (HasCreatedDirectoriesLock)
                        {
                            HasCreatedDirectories.Add(destdir);
                        }
                    }

                    fi.MoveTo(destFileName);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Exception {fi.FullName} {ex}");
                }
            });

            return totalItemsTraversed;
        }
    }
}
