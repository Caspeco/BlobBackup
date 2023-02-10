using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CommandLine;
using CommandLine.Text;

namespace BlobBackup
{
    class CommandOptions
    {
        [Option('n', "accountname", Required = true, HelpText = "Azure account name")]
        public string AccountName { get; set; }

        [Option('k', "accountkey", Required = true, HelpText = "Azure account key")]
        public string AccountKey { get; set; }

        [Option('p', "path", Required = true, HelpText = "Path to local directory where the backups are stored")]
        public string BackupPath { get; set; }

        [Option('c', "container", Required = true, HelpText = "Blob storage container to process")]
        public string ContainerName { get; set; }

        [Option('d', "downloads", Required = false, Default = 40, HelpText = "Number of files to download simultaneously")]
        public int Parallel { get; set; }
    }
}
