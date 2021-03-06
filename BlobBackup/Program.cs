﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BlobBackup
{
    public class Program
    {
        public static int Main(string[] args)
        {
            return MainAsync(args).GetAwaiter().GetResult();
        }

        internal static void PrintStats(Stopwatch sw)
        {
            Console.WriteLine();
            Console.WriteLine($"Elapsed time {sw.Elapsed.ToString()}");
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

                PrintStats(sw);
                Console.WriteLine("Still working ...");

                await processTask;
                sw.Stop();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Something did a bobo {ex.ToString()}");
            }
            job.CheckPrintConsole(true);
            PrintStats(sw);

            Console.WriteLine();
            Console.WriteLine($"Done in {sw.Elapsed.ToString()}");
            if (Debugger.IsAttached) Console.ReadKey();
            return 0;
        }
    }
}
