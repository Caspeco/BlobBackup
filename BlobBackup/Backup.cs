using Microsoft.WindowsAzure.Storage;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace BlobBackup
{
    internal class Backup : IDisposable
    {
        private readonly string _localPath;
        private readonly string _containerName;

        public int TotalItems = 0;
        public long TotalSize = 0;
        public int IgnoredItems = 0;
        public int UpToDateItems = 0;
        public int NewItems = 0;
        public long NewItemsSize = 0;
        public int ModifiedItems = 0;
        public long ModifiedItemsSize = 0;
        public int DownloadedItems = 0;
        public long DownloadedSize = 0;
        public int ExceptionCount = 0;
        public int FailedDownloads = 0;
        public int LocalItems = 0;
        public int DeletedItems = 0;

        private readonly object ExpectedLocalFilesLock = new object();
        private HashSet<string> ExpectedLocalFiles = new HashSet<string>();
        private RunQueue<BlobJob> BlobJobQueue = new RunQueue<BlobJob>();
        private readonly object _tasksListLock = new object();
        private readonly List<Task> _tasks = new List<Task>();
        public int TaskCount => _tasks.Count;

        private static readonly HashSet<char> JobChars = new HashSet<char>();
        private static readonly object JobCharsLock = new object();
        private static DateTime LastConsoleWrite = DateTime.MinValue;
        private static DateTime LastConsoleWriteLine = DateTime.MinValue;
        private static DateTime LastConsoleWriteStats = DateTime.MinValue;

        private FileInfoSqlite _sqlLite;

        public Backup(string localPath, string containerName)
        {
            _localPath = localPath;
            _containerName = containerName;
            _sqlLite = new FileInfoSqlite(containerName, Path.GetFullPath(Path.Combine(_localPath, "..", "sqllite")));
        }

        private const string FLAG_MODIFIED = "[MODIFIED ";
        private const string FLAG_DELETED = "[DELETED ";
        private const string FLAG_DATEFORMAT = "yyyyMMddHHmm";
        private const string FLAG_END = "]";

        internal void AddTasks(params Task[] tasks)
        {
            lock (_tasksListLock)
                _tasks.AddRange(tasks);
        }

        private Task[] GetTasks()
        {
            lock (_tasksListLock)
                return _tasks.ToArray();
        }

        internal async Task WaitTaskAndClean()
        {
            var taskSet = GetTasks();
            if (taskSet.Length == 0)
                return;
            var finishedTask = await Task.WhenAny(taskSet);
            lock (_tasksListLock)
            {
                _tasks.Remove(finishedTask);
                RunQueue<BlobJob>.CleanupTaskList(_tasks);
            }
        }

        private static ParallelQuery<FileInfo> EnumerateFilesParallel(DirectoryInfo dir)
        {
            return dir.EnumerateDirectories()
                .SelectMany(EnumerateFilesParallel)
                .Concat(dir.EnumerateFiles("*", SearchOption.TopDirectoryOnly))
                .AsParallel();
        }

        private bool DoLocalFileDelete(FileSystemInfo f)
        {
            if (f.Name.Contains(FLAG_MODIFIED) || f.Name.Contains(FLAG_DELETED))
                return false;
            if (Interlocked.Increment(ref LocalItems) % 1000 == 0)
            {
                CheckPrintConsole();
            }

            var localFilename = f.FullName; // container is needed as well
            if (localFilename.StartsWith(_localPath))
                localFilename = localFilename.Substring(_localPath.Length + 1);
            return !ExpectedLocalFiles.Contains(localFilename);
    }

        private void AddDownloaded(long size)
        {
            Interlocked.Increment(ref DownloadedItems);
            Interlocked.Add(ref DownloadedSize, size);
        }

        private BlobJob GetBlobJob(BlobItem blob)
        {
            var itemCount = Interlocked.Increment(ref TotalItems);
            if (itemCount % 5000 == 0)
            {
                // set progress JobChar for next console update
                AddJobChar('.');
                CheckPrintConsole();
            }

            if (blob == null)
            {
                Interlocked.Increment(ref IgnoredItems);
                return null;
            }

            Interlocked.Add(ref TotalSize, blob.Size);
            var localFileName = blob.GetLocalFileName();
            var bJob = new BlobJob(this, blob, Path.Combine(_localPath, localFileName));
            if (localFileName == null)
                throw new NullReferenceException();
            lock (ExpectedLocalFilesLock)
                ExpectedLocalFiles.Add(localFileName);

            bJob.FileInfo = _sqlLite.GetFileInfo(blob, bJob.LocalFilePath);
            bJob.AddDownloaded = AddDownloaded;

            return bJob;
        }

        public Backup PrepareJob(string accountName, string accountKey)
        {
            var localContainerPath = Path.Combine(_localPath, _containerName);
            Directory.CreateDirectory(localContainerPath);
            var localDir = new DirectoryInfo(localContainerPath);

            bool? downloadOk = null;
            try
            {
                _sqlLite.BeginTransaction();
                BlobItem.BlobEnumerator(_containerName, accountName, accountKey).Select(GetBlobJob).Where(j => j != null).ForAll(bJob =>
                {
                    var blob = bJob.Blob;
                    var file = bJob.FileInfo;
                    try
                    {
                        if (!file.Exists || bJob.ForceDownloadMissing())
                        {
                            bJob.NeedsJob = JobType.New;
                            bJob.SqlFileInfo.UpdateFromAzure(blob);

                            if (bJob.HandleWellKnownBlob())
                                Interlocked.Increment(ref IgnoredItems);
                            else
                                BlobJobQueue.AddDone(bJob);

                            Interlocked.Increment(ref NewItems);
                            Interlocked.Add(ref NewItemsSize, blob.Size);
                        }
                        else if (!file.IsSame(blob))
                        {
                            /*//
                            if (file.Size != blob.Size ||
                                file.MD5 != blob.MD5 ||
                                file.LastModifiedTimeUtc == blob.LastModifiedTimeUtc) // debug stuff
                                Console.WriteLine($"\n** Seen {file.DiffString(blob)} for {bJob.LocalFilePath}");
                            //*/
                            bJob.NeedsJob = JobType.Modified;
                            BlobJobQueue.AddDone(bJob);
                            Interlocked.Increment(ref ModifiedItems);
                            Interlocked.Add(ref ModifiedItemsSize, blob.Size);
                        }
                        else
                        {
                            Interlocked.Increment(ref UpToDateItems);
                        }
                    }
                    catch (Exception ex)
                    {
                        downloadOk = false;
                        Interlocked.Increment(ref ExceptionCount);
                        Console.WriteLine($"INSIDE LOOP EXCEPTION while scanning {_containerName}. Item: {blob.Uri} Scanned Items: #{TotalItems}. Ex message:" + ex.Message);
                    }
                });
                if (downloadOk == null)
                    downloadOk = true;
            }
            catch (Exception ex)
            {
                downloadOk = false;
                Interlocked.Increment(ref ExceptionCount);
                Console.WriteLine($"OUTER EXCEPTION ({_containerName}) #{TotalItems}: " + ex.Message);
            }
            finally
            {
                _sqlLite.EndTransaction();
                CheckPrintConsole(true);
                Console.WriteLine(" Fetch done");
            }

            var nowUtc = DateTime.UtcNow;
            var delTask = Task.Run(() =>
            {
                if (!downloadOk.HasValue || !downloadOk.Value)
                {
                    Console.WriteLine(" Due to exception, no delete check will be done");
                    return;
                }
                Console.WriteLine(" Starting delete files known in local sql but not in azure");
                _sqlLite.GetAllFileInfos().AsParallel().
                    Where(fi => !ExpectedLocalFiles.Contains(fi.LocalName)).
                    ForAll(fileInfo =>
                {
                    AddJobChar('d');
                    fileInfo.DeleteDetectedTime = nowUtc;
                    fileInfo.UpdateDb();
                    string fileName = Path.Combine(_localPath, fileInfo.LocalName);
                    var fi = new FileInfo(fileName);

                    var newName = fileName + FLAG_DELETED + nowUtc.ToString(FLAG_DATEFORMAT) + FLAG_END;
                    if (fi.Exists)
                        fi.MoveTo(newName);
                    else if (Directory.Exists(Path.GetDirectoryName(newName)))
                        File.Create(newName + ".empty").Close(); // creates dummy file to mark as deleted
                    Interlocked.Increment(ref DeletedItems);
                });
                CheckPrintConsole(true);
                Console.WriteLine(" Delete files known in local sql but not in azure done");
                Console.WriteLine(" Starting delete existing local files not in azure");

                // scan for deleted files by checking if we have a file locally that we did not find remotely
                // load list of local files
                // this might take a minute or 2 if many files, since we wait for first yielded item before continuing
                // done after sql Loop, since that should "remove" them already
                EnumerateFilesParallel(localDir).
                    Where(DoLocalFileDelete).
                    ForAll(fi =>
                {
                    AddJobChar('D');
                    fi.MoveTo(fi.FullName + FLAG_DELETED + nowUtc.ToString(FLAG_DATEFORMAT) + FLAG_END);
                    Interlocked.Increment(ref DeletedItems);
                });
                CheckPrintConsole(true);
                Console.WriteLine(" Delete existing local files not in azure done");
            });
            AddTasks(delTask);
            BlobJobQueue.RunnerDone();

            return this;
        }

        internal static bool AddJobChar(char j)
        {
            lock (JobCharsLock)
            {
                return JobChars.Add(j);
            }
        }

        internal bool CheckPrintConsole(bool forceFull = false)
        {
            var utcNow = DateTime.UtcNow;

            char[] jChars = {};
            if (JobChars.Count != 0)
                lock (JobCharsLock)
                {
                    jChars = JobChars.ToArray();
                    JobChars.Clear();
                }

            if (jChars.Length > 0 && LastConsoleWrite < utcNow.AddSeconds(-10))
            {
                // don't spam console to much, here we print the last Job item we dealt with
                LastConsoleWrite = utcNow;
                Console.Write(string.Join(string.Empty, jChars));
            }

            if (forceFull || LastConsoleWriteLine < utcNow.AddMinutes(-0.5))
            {
                LastConsoleWriteLine = utcNow;
                Console.WriteLine("\n --MARK-- " + utcNow.ToString("yyyy-MM-dd HH:mm:ss.ffff") + $" - Currently {TotalItems.Format()} scanned, {TaskCount.Format()} tasks, {BlobJobQueue.QueueCount.Format()} waiting jobs");
                if (forceFull || LastConsoleWriteStats < utcNow.AddMinutes(-2))
                {
                    LastConsoleWriteStats = utcNow;
                    PrintStats();
                }
                Console.Out.Flush();

                return true;
            }

            return jChars.Length > 0;
        }

        public void PrintStats()
        {
            Console.WriteLine($" {TotalItems.Format()} remote items scanned, total size {TotalSize.FormatSize()} and found:");
            Console.WriteLine($" {NewItems.Format()} new files. Total size {NewItemsSize.FormatSize()}");
            Console.WriteLine($" {ModifiedItems.Format()} modified files. Total size {ModifiedItemsSize.FormatSize()}");
            Console.WriteLine($" {DownloadedItems.Format()} downloaded files. Total size {DownloadedSize.FormatSize()}");
            Console.WriteLine($" {UpToDateItems.Format()} files up to date");
            Console.WriteLine($" {IgnoredItems.Format()} ignored items, {FailedDownloads.Format()} failed, {ExceptionCount.Format()} exceptions");
            Console.WriteLine($" {LocalItems.Format()} local items");
            Console.WriteLine($" {DeletedItems.Format()} local files deleted (or moved)");
        }

        public async Task ProcessJob(int simultaniousDownloads)
        {
            try
            {
                foreach (var item in BlobJobQueue.GetDoneEnumerable())
                {
                    if (TaskCount >= simultaniousDownloads)
                    {
                        CheckPrintConsole();
                        await WaitTaskAndClean();
                    }

                    AddTasks(Task.Run(item.DoJob));
                }

                CheckPrintConsole(true);

                while (TaskCount > 0)
                {
                    await Task.WhenAll(GetTasks());
                    await WaitTaskAndClean();
                }
            }
            finally
            {
                _sqlLite.Dispose();
                _sqlLite = null;
                CheckPrintConsole(true);
            }
        }

        internal enum JobType
        {
            None = 0,
            New = 1,
            Modified = 2,
        }

        public class BlobJob
        {
            internal readonly Backup Bak;
            internal readonly BlobItem Blob;
            internal readonly string LocalFilePath;
            internal ILocalFileInfo FileInfo;
            internal Action<long> AddDownloaded;
            internal FileInfoSqlite.FileInfo SqlFileInfo => FileInfo as FileInfoSqlite.FileInfo;
            internal JobType NeedsJob = JobType.None;

            private static readonly HashSet<string> HasCreatedDirectories = new HashSet<string>();

            public BlobJob(Backup bakParent, BlobItem blob, string localFilePath)
            {
                Bak = bakParent;
                Blob = blob;
                LocalFilePath = localFilePath;
            }

            private static DateTime ForceExistsFrom = DateTime.Now.AddDays(-30);
            public bool ForceDownloadMissing()
            {
                if (FileInfo.Size != Blob.Size || FileInfo.MD5 != Blob.MD5)
                    return false; // if file changed, expect it to be downloaded by modification instad

                var lfi = SqlFileInfo?.SrcFileInfo;
                if (!(lfi is LocalFileInfoDisk))
                    return false;

                // if newer than a month, and no file exits, force it
                if (Blob.LastModifiedTimeUtc > ForceExistsFrom &&
                    !lfi.Exists && !WellKnownBlob(Blob))
                {
                    Console.WriteLine($"\n** Force Download expected existing file {Blob.ToString()}");
                    return true;
                }

                return false;
            }

            public static bool WellKnownBlob(ILocalFileInfo blob)
            {
                // Ignore empty files
                if (blob.Size == 0)
                    return true;

                if (blob.MD5 == "1B2M2Y8AsgTpgAmY7PhCfg==" && blob.Size < 1024 * 1024) // md5 same as 0 byte
                    return true;

                // Ignore files only containing "[]"
                if (blob.Size == 2 && blob.MD5 == "11FxOYiYfpMxmANj4kGJzg==")
                    return true;

                return false;
            }

            public bool HandleWellKnownBlob()
            {
                if (!WellKnownBlob(Blob))
                    return false;
                // no real download of these files
                SqlFileInfo.LastDownloadedTime = DateTime.UtcNow;
                SqlFileInfo.UpdateDb();
                NeedsJob = JobType.None;
                return true;
            }

            public async Task<bool> DoJob()
            {
                try
                {
                    LocalFileInfoDisk lfi = null;
                    if (NeedsJob == JobType.New)
                    {
                        AddJobChar('N');
                        var dir = Path.GetDirectoryName(LocalFilePath);
                        if (!HasCreatedDirectories.Contains(dir))
                        {
                            Directory.CreateDirectory(dir);
                            HasCreatedDirectories.Add(dir);
                        }
                    }
                    else if (NeedsJob == JobType.Modified)
                    {
                        AddJobChar('m');
                        lfi = new LocalFileInfoDisk(LocalFilePath);
                        var noDownloadNeeded =
                            FileInfo.Size == Blob.Size &&
                            FileInfo.MD5 == Blob.MD5 &&
                            (!lfi.Exists || (lfi.Size == Blob.Size && lfi.GetMd5() == Blob.MD5));
                        /*//
                        Console.WriteLine($"\n** Handling {FileInfo.DiffString(Blob)} for {LocalFilePath}");
                        Console.WriteLine($"\n** Handling2 {lfi.DiffString(Blob)} for {LocalFilePath}");
                        //*/
                        if (noDownloadNeeded)
                        {
                            // since size and hash is the same as last, we just fix local modification time and update database
                            SqlFileInfo.UpdateDb();
                            lfi.UpdateWriteTime(Blob.LastModifiedTimeUtc);
                            return true;
                        }

                        if (lfi.Exists)
                        {
                            try
                            {
                                if (lfi.Size <= 0) // just remove empty files, shouldn't exist
                                    lfi.fInfo.Delete();
                                else
                                {
                                    var dst = LocalFilePath + FLAG_MODIFIED + Blob.LastModifiedUtc.ToString(FLAG_DATEFORMAT) + FLAG_END;
                                    if (File.Exists(dst))
                                    {
                                        File.Delete(dst);
                                    }

                                    lfi.fInfo.MoveTo(dst);
                                }
                            }
                            catch (IOException)
                            {
                                Interlocked.Increment(ref Bak.ExceptionCount);
                                // ignore
                            }
                        }
                    }
                    else
                    {
                        return true;
                    }

                    if (HandleWellKnownBlob())
                        return true;

                    SqlFileInfo.UpdateFromAzure(Blob);
                    await Blob.DownloadToFileAsync(LocalFilePath, FileMode.Create);
                    SqlFileInfo.LastDownloadedTime = DateTime.UtcNow;
                    AddDownloaded(Blob.Size);
                    // we always want a new file item after download
                    lfi = new LocalFileInfoDisk(LocalFilePath);
                    lfi.UpdateWriteTime(Blob.LastModifiedTimeUtc);

                    // maybe something changed from orignal data
                    if (lfi.Size != Blob.Size || lfi.GetMd5() != Blob.MD5)
                    {
                        Interlocked.Increment(ref Bak.FailedDownloads);
                        return false; // something went bad, we can try on next run if db isn't updated
                    }

                    SqlFileInfo.UpdateFromFileInfo(lfi);
                    SqlFileInfo.UpdateDb();

                    NeedsJob = JobType.None;
                    return true;
                }
                catch (StorageException ex)
                {
                    Interlocked.Increment(ref Bak.ExceptionCount);
                    // Swallow 404 exceptions.
                    // This will happen if the file has been deleted in the temporary period from listing blobs and downloading
                    Console.WriteLine("\nSwallowed Ex: " + LocalFilePath + " " + ex.ToString());
                }
                catch (IOException ex)
                {
                    HasCreatedDirectories.Clear();
                    Interlocked.Increment(ref Bak.ExceptionCount);
                    Console.WriteLine("\nSwallowed Ex: " + LocalFilePath + " " + ex.ToString());
                }
                return false;
            }
        }

        #region IDisposable Support
        private bool disposedValue = false; // To detect redundant calls

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    var sqlInstance = _sqlLite;
                    if (sqlInstance != null)
                    {
                        sqlInstance.Dispose();
                        _sqlLite = null;
                    }

                    var runQ = BlobJobQueue;
                    if (runQ != null)
                    {
                        runQ.Dispose();
                        BlobJobQueue = null;
                    }
                }
                ExpectedLocalFiles = null;

                disposedValue = true;
            }
        }

        // This code added to correctly implement the disposable pattern.
        void IDisposable.Dispose()
        {
            Dispose(true);
        }
        #endregion
    }
}
