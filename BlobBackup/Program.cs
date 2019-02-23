using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BlobBackup
{
    public class Program
    {
        private static string FormatSize(long size)
        {
            string[] sizes = { "B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB" };
            int order = 0;
            while (size >= 1024 && order < sizes.Length - 1)
            {
                order++;
                size = size / 1024;
            }

            // Adjust the format string to your preferences. For example "{0:0.#}{1}" would
            // show a single decimal place, and no space.
            return string.Format("{0:0.##} {1}", size, sizes[order]);
        }

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

            var job = new Backup(options.BackupPath, options.ContainerName);

            Console.WriteLine("Scanning and processing remote items ");
            var sw = Stopwatch.StartNew();

            var prepTask = Task.Run(() => job.PrepareJob(options.AccountName, options.AccountKey));
            job.AddTasks(prepTask);
            var processTask = job.ProcessJob(options.Parallel);
            await prepTask;

            Console.WriteLine();
            Console.WriteLine($"Scanned {job.ScannedItems} remote items and found:");
            Console.WriteLine($"{job.NewItems} new files. Total size {FormatSize(job.NewItemsSize)}.");
            Console.WriteLine($"{job.ModifiedItems} modified files. Total size {FormatSize(job.ModifiedItemsSize)}.");
            Console.WriteLine($"{job.UpToDateItems} files up to date.");
            Console.WriteLine($"{job.IgnoredItems} ignored items.");
            Console.Write("Still working ...");
            job.CheckPrintConsole(true);

            await processTask;
            sw.Stop();
            Console.WriteLine();
            Console.Write($"{job.DeletedItems} files deleted.");
            job.CheckPrintConsole(true);

            Console.WriteLine();
            Console.WriteLine($"Done in {sw.Elapsed.ToString()}");
            if (Debugger.IsAttached) Console.ReadKey();
            return 0;
        }
    }
}
