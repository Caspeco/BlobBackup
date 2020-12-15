using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace DuplicateFinder
{
    public class DuplicateFinderMain
    {
        public static int Main(string[] args)
        {
            return MainAsync(args).GetAwaiter().GetResult();
        }

        public static async Task<int> MainAsync(string[] args)
        {
            var sw = Stopwatch.StartNew();
            var items = await FindFiles(args[0]);
            sw.Stop();

            Console.WriteLine($"Duplicate done {items} items traversed in {sw.Elapsed.ToString()}");

            if (Debugger.IsAttached) Console.ReadKey();
            return 0;
        }

        private abstract class TreeNodeBase
        {
            private static readonly object Lock = new object();
            public DupItem Existing { get; internal set; }

            protected abstract TreeNodeDupMd5Full AddNonFirst(DupItem d);

            public TreeNodeDupMd5Full Add(DupItem d)
            {
                lock (Lock)
                {
                    if (Existing != null)
                        return AddNonFirst(d);

                    Existing = d;
                    return null;
                }
            }
        }

        private class TreeNodeDupSize : TreeNodeBase
        {
            private readonly Dictionary<string, TreeNodeDupMd5OneBlock> SubTree = new Dictionary<string, TreeNodeDupMd5OneBlock>();

            protected override TreeNodeDupMd5Full AddNonFirst(DupItem d)
            {
                var unhandSibs = SubTree.Count == 0 ? new[] {Existing, d} : new[] {d};
                return unhandSibs
                    .Select(s => SubTree.GetOrNew(s.GetMd5OneBlock()).Add(s))
                    .Last();
            }
        }

        private class TreeNodeDupMd5OneBlock : TreeNodeBase
        {
            private readonly Dictionary<string, TreeNodeDupMd5Full> SubTree = new Dictionary<string, TreeNodeDupMd5Full>();

            protected override TreeNodeDupMd5Full AddNonFirst(DupItem d)
            {
                var unhandSibs = SubTree.Count == 0 ? new[] {Existing, d} : new[] {d};
                return unhandSibs
                    .Select(s => SubTree.GetOrNew(s.GetMd5Full()).Add(s))
                    .Last();
            }
        }

        private class TreeNodeDupMd5Full : TreeNodeBase
        {
            protected override TreeNodeDupMd5Full AddNonFirst(DupItem d)
            {
                return this;
            }
        }

        // Could this be a binary search tree and be faster/more efficient?
        private static readonly Dictionary<long, TreeNodeDupSize> Root = new Dictionary<long, TreeNodeDupSize>();
        private static readonly object RootLock = new object();

        private static Tuple<DupItem, TreeNodeDupMd5Full> AddDup(FileInfo fInfo)
        {
            var d = new DupItem(fInfo);
            var s = d.Size;
            var n = Root.Get(s);
            if (n == null)
            {
                lock (RootLock)
                    n = Root.GetOrNew(s);
            }

            var tFull = n.Add(d);

            return Tuple.Create(d, tFull);
        }

        public static async Task<long> FindFiles(string path)
        {
            // building a tree of size, small size checksum, full size
            // only moving on to the next step if current step has duplicate

            var tasks = new System.Collections.Concurrent.ConcurrentBag<Task>();

            long totalItemsTraversed = 0;
            int itemsSincePrint = 0;
            var runStart = DateTime.UtcNow;
            var dir = new DirectoryInfo(path);
            Tools.EnumerateFilesParallel(dir)
                .Where(fi => fi.LastWriteTimeUtc < runStart.AddHours(-1))
                .Select(AddDup).ForAll(dnTpl =>
            {
                Interlocked.Increment(ref totalItemsTraversed);
                if (Interlocked.Increment(ref itemsSincePrint) % 10000 == 1)
                {
                    Console.Write('.');
                }

                var tNode = dnTpl.Item2;
                if (tNode == null)
                    return; // not duplicate
                var d = dnTpl.Item1;
                try
                {
                    //  TODO try to be smart, use HardLink list to avoid checking different files
                    if (d.HasHardLink)
                    {
                        var existing = tNode.Existing;
                        if (existing.HasHardLink)
                            return;
                        tNode.Existing = d;
                        d = existing;
                    }

                    Console.WriteLine($"{DateTime.UtcNow:yyyy-MM-dd HH:mm:ss.fff} Duplicate {d}");
                    itemsSincePrint = 0;
                    var linkItem = tNode.Existing;
                    linkItem.FileInfo.CreateHardLink(d.FileInfo.FullName);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Exception {d.FileInfo.FullName} " + ex.ToString());
                }
            });

            await Task.WhenAll(tasks);

            return totalItemsTraversed;
        }
    }
}
