﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BlobBackup
{
    class Program
    {
        // todo:
        // Logga till en fil så att vi kan se hur det gick
        // try catch ? Hur sköter vi TransientErrors
        // Jämföra checksum för att inte ladda hem samma content två ggr
        // Skicka mejl eller på något annat sätt signalera att det funkar som det ska
        // ladda hem async och parralelt
        // DONE - timings
        // fler containers (samtliga)

        static string FormatSize(long size)
        {
            string[] sizes = { "B", "KB", "MB", "GB", "TB" };
            int order = 0;
            while (size >= 1024 && order < sizes.Length - 1)
            {
                order++;
                size = size / 1024;
            }

            // Adjust the format string to your preferences. For example "{0:0.#}{1}" would
            // show a single decimal place, and no space.
            return String.Format("{0:0.##} {1}", size, sizes[order]);
        }

        static void Main(string[] args)
        {
            var options = new CommandOptions();

            if (!CommandLine.Parser.Default.ParseArguments(args, options))
            {
                Environment.Exit(CommandLine.Parser.DefaultExitCodeFail);
            }

            var backup = new Backup(options.BackupPath);

            Console.WriteLine("Scanning remote items...");
            var sw = Stopwatch.StartNew();

            var job = backup.PrepareJob(options.ContainerName, options.AccountName, options.AccountKey, new Progress<int>(v => { Console.WriteLine("Processed: " + v); }));

            Console.WriteLine($"Scanned {job.ScannedItems} remote items.");
            Console.WriteLine($"{job.NewFiles.Count} new files. Total size {FormatSize(job.NewFilesSize)}.");
            Console.WriteLine($"{job.ModifiedFiles.Count} modified files. Total size {FormatSize(job.ModifiedFilesSize)}.");
            Console.WriteLine($"{job.UpToDateItems} files up to date.");
            Console.WriteLine($"{job.DeletedFiles.Count} files deleted.");
            Console.WriteLine($"{job.IgnoredItems} ignored items.");
            Console.WriteLine("Downloading...");

            backup.ProcessJob(job, options.Parallel).Wait();

            sw.Stop();
            Console.WriteLine();
            Console.WriteLine($"Done in {sw.Elapsed.ToString()}.");
            //Console.ReadKey();
        }
    }
}