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

            Console.WriteLine($"\nDuplicate done {items} items traversed in {sw.Elapsed.ToString()}");

            if (Debugger.IsAttached) Console.ReadKey();
            return 0;
        }

        private abstract class TreeNodeBase
        {
            private static readonly object Lock = new object();
            public DupItem Existing { get; internal set; }

            protected virtual string KeyBeforeLock(DupItem x) { return null; }
            protected abstract IList<Tuple<TreeNodeBase, DupItem>> AddNonFirstLock(DupItem d);
            protected virtual TreeNodeDupMd5Full AddNonFirstPostLock(IList<Tuple<TreeNodeBase, DupItem>> l)
            {
                return l
                    .Select(t => t.Item1.Add(t.Item2))
                    .Last();
            }

            public TreeNodeDupMd5Full Add(DupItem d)
            {
                if (Existing != null)
                    KeyBeforeLock(d); // do this outside lock

                IList<Tuple<TreeNodeBase, DupItem>> l;
                lock (Lock)
                {
                    if (Existing == null)
                    {
                        Existing = d;
                        return null;
                    }
                    l = AddNonFirstLock(d); // do lock critical
                }

                return AddNonFirstPostLock(l); // do stuff not needing lock at this level
            }
        }

        private class TreeNodeDupSize : TreeNodeBase
        {
            private readonly Dictionary<string, TreeNodeDupMd5OneBlock> SubTree = new Dictionary<string, TreeNodeDupMd5OneBlock>();

            protected override string KeyBeforeLock(DupItem x)
            {
                return x.GetMd5OneBlock();
            }

            protected override IList<Tuple<TreeNodeBase, DupItem>> AddNonFirstLock(DupItem d)
            {
                var unhandSibs = SubTree.Count == 0 ? new[] {Existing, d} : new[] {d};
                return unhandSibs
                    .Select(s => Tuple.Create((TreeNodeBase)SubTree.GetOrNew(KeyBeforeLock(s)), s))
                    .ToList();
            }
        }

        private class TreeNodeDupMd5OneBlock : TreeNodeBase
        {
            private readonly Dictionary<string, TreeNodeDupMd5Full> SubTree = new Dictionary<string, TreeNodeDupMd5Full>();

            protected override string KeyBeforeLock(DupItem x)
            {
                return x.GetMd5Full();
            }

            protected override IList<Tuple<TreeNodeBase, DupItem>> AddNonFirstLock(DupItem d)
            {
                var unhandSibs = SubTree.Count == 0 ? new[] {Existing, d} : new[] {d};
                return unhandSibs
                    .Select(s => Tuple.Create((TreeNodeBase)SubTree.GetOrNew(KeyBeforeLock(s)), s))
                    .ToList();
            }
        }

        private class TreeNodeDupMd5Full : TreeNodeBase
        {
            protected override IList<Tuple<TreeNodeBase, DupItem>> AddNonFirstLock(DupItem d)
            {
                return null;
            }

            protected override TreeNodeDupMd5Full AddNonFirstPostLock(IList<Tuple<TreeNodeBase, DupItem>> l)
            {
                return this;
            }
        }

        // Could this be a binary search tree and be faster/more efficient?
        private static readonly Dictionary<long, TreeNodeDupSize> Root = new Dictionary<long, TreeNodeDupSize>();
        private static readonly object RootLock = new object();

        private static Tuple<DupItem, TreeNodeDupMd5Full> AddDup(FileInfo fInfo, Action<DupItem> doLink)
        {
            var d = new DupItem(fInfo);
            var s = d.Size;
            var n = Root.Get(s);
            if (n == null)
            {
                lock (RootLock)
                    n = Root.GetOrNew(s);
            }

            Task.Run(() => doLink(d));

            var tFull = n.Add(d);

            return Tuple.Create(d, tFull);
        }

        public static async Task<long> FindFiles(string path)
        {
            // building a tree of size, small size checksum, full size
            // only moving on to the next step if current step has duplicate

            var tasks = new System.Collections.Concurrent.ConcurrentBag<Task>();
            var knownHardLinks = new System.Collections.Concurrent.ConcurrentDictionary<string, byte>();
            long totalItemsTraversed = 0;
            var runStart = DateTime.UtcNow;
            var dir = new DirectoryInfo(path);
            var stripPath = dir.FullName;
            var root = Path.GetPathRoot(stripPath);
            if (stripPath.StartsWith(root))
                stripPath = @"\" + stripPath.Substring(root.Length);

            char pChar = 's';

            // if file is known among hardlinks then there is no need to check it further
            bool HasNoLink(FileInfo fi)
            {
                if (Interlocked.Increment(ref totalItemsTraversed) % (pChar == '.' ? 5000 : 100) == 0)
                {
                    Console.Write(pChar);
                    pChar = '.';
                }
                var k = fi.FullName.Substring(path.Length);
                var isKnownLink = knownHardLinks.ContainsKey(k);
                if (isKnownLink)
                {
                    knownHardLinks.TryRemove(k, out var b); // faster to only check this? since it returns true if removed successfully?
                    pChar = '-';
                }

                return !isKnownLink;
            }

            void DoLinks(DupItem d)
            {
                var hl = d.GetHardLinks(stripPath);
                var k = d.FileInfo.FullName.Substring(path.Length);
                foreach (var l in hl.Except(new[] {k}))
                    knownHardLinks.TryAdd(l, (byte)0);
            }

            Tuple<DupItem, TreeNodeDupMd5Full> AddDupInt(FileInfo fi)
            {
                return AddDup(fi, DoLinks);
            }

            Tools.EnumerateFilesParallel(dir)
                .Where(HasNoLink)
                .Where(fi => fi.LastWriteTimeUtc < runStart.AddHours(-1))
                .Select(AddDupInt)
                .ForAll(dnTpl =>
            {
                var tNode = dnTpl.Item2;
                if (tNode == null)
                    return; // not duplicate
                var d = dnTpl.Item1;
                try
                {
                    if (d.HasHardLink)
                    {
                        pChar = 'x';
                        var existing = tNode.Existing;
                        if (existing.HasHardLink)
                        {
                            pChar = 'r';
                            return;
                        }

                        tNode.Existing = d;
                        d = existing;
                    }
                    // TODO if we have 2 identical files, and both of them has links (not same), what to do? (to avoid having 2 separate identical sets)
                    // TODO there is a limit on how many references can be done to the same data?

                    pChar = 'l';
                    Console.WriteLine($" {DateTime.UtcNow:yyyy-MM-dd HH:mm:ss.fff} Duplicate {d}");
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
