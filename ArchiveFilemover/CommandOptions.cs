using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CommandLine;
using CommandLine.Text;

namespace ArchiveFilemover
{
    class CommandOptions
    {
        [Option('s', "source", Required = true, HelpText = "Path to local directory where files to be backuped are stored")]
        public string SourcePath { get; set; }

        [Option('d', "destination", Required = true, HelpText = "Path to where moved files should be placed")]
        public string DestinationPath { get; set; }

        [Option('x', "maxwritedate", HelpText = "Ignore files with write time after this")]
        public DateTime MaxWriteTime { get; set; }

        [HelpOption]
        public string GetUsage()
        {
            return HelpText.AutoBuild(this, current => HelpText.DefaultParsingErrorsHandler(this, current));
        }
    }
}
