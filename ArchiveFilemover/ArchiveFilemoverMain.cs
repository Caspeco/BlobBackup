using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ArchiveFilemover
{
    public class ArchiveFilemoverMain
    {
        public static int Main(string[] args)
        {
            return MainAsync(args).GetAwaiter().GetResult();
        }

        public static async Task<int> MainAsync(string[] args)
        {
            var options = new CommandOptions();
            if (!CommandLine.Parser.Default.ParseArguments(args, options))
            {
                Environment.Exit(CommandLine.Parser.DefaultExitCodeFail);
            }

            var sw = Stopwatch.StartNew();
            var items = await MoveFiles(options);
            sw.Stop();

            Console.WriteLine($"Move done {items} items traversed in {sw.Elapsed.ToString()}");

            sw = Stopwatch.StartNew();
            Console.WriteLine("Removing empty dirs...");
            DeleteEmptyDirs(options.SourcePath);
            sw.Stop();
            Console.WriteLine($"Empty removal done after {sw.Elapsed.ToString()}");

            if (Debugger.IsAttached) Console.ReadKey();
            return 0;
        }

        public static bool DeleteEmptyDirs(string dir)
        {
            bool noRemainingDirs = true;
            // walk all subdirs, and recursively remove
            foreach (var d in Directory.EnumerateDirectories(dir))
            {
                noRemainingDirs = DeleteEmptyDirs(d) & noRemainingDirs;
            }
            // if subdir failed removal, or there is any files in the dir return as not deleted
            if (!noRemainingDirs || Directory.EnumerateFileSystemEntries(dir).Any())
                return false;

            try
            {
                // try to delete, and if ok return true for delete ok
                Directory.Delete(dir);
                Console.WriteLine(dir);
                return true;
            }
            catch (UnauthorizedAccessException) { }
            catch (DirectoryNotFoundException) { }
            // failure to delete is false
            return false;
        }

        private static readonly HashSet<string> HasCreatedDirectories = new HashSet<string>();

        private static async Task<long> MoveFiles(CommandOptions options)
        {
            long totalItemsTraversed = 0;
            int itemsSincePrint = 0;
            var runStart = DateTime.UtcNow;
            var runStartString = runStart.ToString("yyyyMMdd_HHmm");
            var hasMaxWriteTimeOption = options.MaxWriteTime != DateTime.MinValue;
            var dir = new DirectoryInfo(options.SourcePath);
            foreach (var fi in dir.EnumerateFiles("*", SearchOption.AllDirectories))
            {
                totalItemsTraversed++;
                itemsSincePrint++;
                if (itemsSincePrint % 10000 == 1)
                {
                    Console.Write('.');
                }

                try
                {
                    var lastWriteUtc = fi.LastWriteTimeUtc;
                    if (hasMaxWriteTimeOption && lastWriteUtc > options.MaxWriteTime)
                        continue;

                    var destName = fi.FullName.Substring(options.SourcePath.Length + 1);
                    var destFileName = Path.Combine(options.DestinationPath, runStartString + "_modyear_" + lastWriteUtc.Year, destName);
                    var destdir = Path.GetDirectoryName(destFileName);

                    var printDest = destdir.Substring(options.DestinationPath.Length + 1);
                    Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff")} {destName} => {printDest}");
                    itemsSincePrint = 0;

                    if (!HasCreatedDirectories.Contains(destdir))
                    {
                        Directory.CreateDirectory(destdir);
                        HasCreatedDirectories.Add(destdir);
                    }

                    fi.MoveTo(destFileName);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Exception {fi.FullName} " + ex.ToString());
                }
            }

            return totalItemsTraversed;
        }
    }
}
