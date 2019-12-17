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
        private enum FileSizeUnit : byte
        {
            B, KB, MB,
            GB, TB, PB,
            EB, ZB, YB,
        }

        public static string FormatSize(double size)
        {
            var unit = FileSizeUnit.B;
            while (size >= 1024 && unit < FileSizeUnit.YB)
            {
                size = size / 1024;
                unit++;
            }
            return string.Format("{0:#,##0.##} {1}", size, unit);
        }

        public static int Main(string[] args)
        {
            return MainAsync(args).GetAwaiter().GetResult();
        }

        internal static void PrintStats(Stopwatch sw, Backup job)
        {
            Console.WriteLine();
            Console.WriteLine($"Elapsed time {sw.Elapsed.ToString()}");
            Console.WriteLine($" {job.TotalItems} remote items scanned, total size {FormatSize(job.TotalSize)} and found:");
            Console.WriteLine($" {job.NewItems} new files. Total size {FormatSize(job.NewItemsSize)}");
            Console.WriteLine($" {job.ModifiedItems} modified files. Total size {FormatSize(job.ModifiedItemsSize)}");
            Console.WriteLine($" {job.DownloadedItems} downloaded files. Total size {FormatSize(job.DownloadedSize)}");
            Console.WriteLine($" {job.UpToDateItems} files up to date");
            Console.WriteLine($" {job.IgnoredItems} ignored items");
            Console.WriteLine($" {job.LocalItems} local items");
            Console.WriteLine($" {job.DeletedItems} local files deleted (or moved)");
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

            try
            {
                var prepTask = Task.Run(() => job.PrepareJob(options.AccountName, options.AccountKey));
                job.AddTasks(prepTask);
                var processTask = job.ProcessJob(options.Parallel);
                await prepTask;

                PrintStats(sw, job);
                Console.WriteLine("Still working ...");
                job.CheckPrintConsole(true);

                await processTask;
                sw.Stop();
                PrintStats(sw, job);
                job.CheckPrintConsole(true);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Something did a bobo {ex.ToString()}");
                job.CheckPrintConsole(true);
            }

            Console.WriteLine();
            Console.WriteLine($"Done in {sw.Elapsed.ToString()}");
            if (Debugger.IsAttached) Console.ReadKey();
            return 0;
        }
    }
}
