using Microsoft.WindowsAzure.Storage;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace BlobBackup
{
    internal class Backup : IDisposable
    {
        private readonly string _localPath;
        private readonly string _containerName;

        public int ScannedItems = 0;
        public int IgnoredItems = 0;
        public int UpToDateItems = 0;
        public int NewItems = 0;
        public int ModifiedItems = 0;
        public int DeletedItems = 0;
        public long NewItemsSize = 0;
        public long ModifiedItemsSize = 0;

        private HashSet<string> ExpectedLocalFiles = new HashSet<string>();
        private RunQueue<BlobJob> BlobJobQueue = new RunQueue<BlobJob>();
        private readonly object _tasksListLock = new object();
        private List<Task> _tasks = new List<Task>();
        public int TaskCount => _tasks.Count;

        private static readonly HashSet<char> JobChars = new HashSet<char>();
        private static readonly object JobCharsLock = new object();
        private static DateTime LastConsoleWrite = DateTime.MinValue;
        private static DateTime LastConsoleWriteLine = DateTime.MinValue;

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
            var finishedTask = taskSet.Length > 0 ? await Task.WhenAny(taskSet) : null;
            lock (_tasksListLock)
            {
                _tasks.Remove(finishedTask);
                RunQueue<BlobJob>.CleanupTaskList(_tasks);
            }
        }

        private static ParallelQuery<FileInfo> EnumerateFilesParallel(DirectoryInfo dir)
        {
            return dir.EnumerateDirectories()
                .AsParallel()
                .SelectMany(EnumerateFilesParallel)
                .Concat(dir.EnumerateFiles("*", SearchOption.TopDirectoryOnly).AsParallel());
        }

        public Backup PrepareJob(string accountName, string accountKey)
        {
            var localContainerPath = Path.Combine(_localPath, _containerName);
            Directory.CreateDirectory(localContainerPath);
            var localDir = new DirectoryInfo(localContainerPath);

            // load list of local files
            // this might take a minute or 2 if many files, since we wait for first yielded item before continueing
            var localFileList = EnumerateFilesParallel(localDir).
                    Where(f => !f.Name.Contains(FLAG_MODIFIED) && !f.Name.Contains(FLAG_DELETED));

            try
            {
                _sqlLite.BeginTransaction();
                foreach (var blob in BlobItem.BlobEnumerator(_containerName, accountName, accountKey))
                {
                    ScannedItems++;
                    try
                    {
                        if (ScannedItems % 10000 == 0)
                        {
                            // set progress JobChar for next console update
                            AddJobChar('.');
                            CheckPrintConsole();
                        }

                        if (blob == null)
                        {
                            IgnoredItems++;
                            continue;
                        }

                        var localFileName = blob.GetLocalFileName();
                        var bJob = new BlobJob(blob, Path.Combine(_localPath, localFileName));
                        ExpectedLocalFiles.Add(localFileName);

                        ILocalFileInfo file = _sqlLite.GetFileInfo(blob, bJob.LocalFilePath);
                        bJob.FileInfo = file;
                        if (!file.Exists)
                        {
                            bJob.NeedsJob = JobType.New;
                            BlobJobQueue.AddDone(bJob);
                            NewItems++;
                            NewItemsSize += blob.Size;
                        }
                        else if (file.Size != blob.Size || file.LastWriteTimeUtc < blob.LastModifiedUtc.UtcDateTime ||
                            (file.MD5 != null && !string.IsNullOrEmpty(blob.MD5) && file.MD5 != blob.MD5))
                        {
                            bJob.NeedsJob = JobType.Modified;
                            BlobJobQueue.AddDone(bJob);
                            ModifiedItems++;
                            ModifiedItemsSize += blob.Size;
                        }
                        else
                        {
                            UpToDateItems++;
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"INSIDE LOOP EXCEPTION while scanning {_containerName}. Item: {blob.Uri} Scanned Items: #{ScannedItems}. Ex message:" + ex.Message);
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"OUTER EXCEPTION ({_containerName}) #{ScannedItems}: " + ex.Message);
            }
            _sqlLite.EndTransaction();
            CheckPrintConsole(true);
            Console.WriteLine(" Fetch done");

            var nowUtc = DateTime.UtcNow;
            var fileDelTask = Task.Run(async () =>
            {
                // scan for deleted files by checking if we have a file locally that we did not find remotely
                localFileList.ForAll(file =>
                {
                    var localFilename = file.FullName; // container is needed as well
                    if (localFilename.StartsWith(_localPath)) localFilename = localFilename.Substring(_localPath.Length + 1);
                    if (ExpectedLocalFiles.Contains(localFilename))
                        return;
                    AddJobChar('D');
                    string fileName = Path.Combine(_localPath, localFilename);
                    file.MoveTo(fileName + FLAG_DELETED + nowUtc.ToString(FLAG_DATEFORMAT) + FLAG_END);
                    Interlocked.Increment(ref DeletedItems);
                });
                CheckPrintConsole(true);
                Console.WriteLine(" Delete existing local files not in azure done");
            });

            var sqlDelTask = Task.Run(() =>
            {
                foreach (var fileInfo in _sqlLite.GetAllFileInfos().
                    AsParallel().Where(fi => !ExpectedLocalFiles.Contains(fi.LocalName)))
                {
                    AddJobChar('d');
                    fileInfo.DeleteDetectedTime = nowUtc;
                    string fileName = Path.Combine(_localPath, fileInfo.LocalName);

                    var newName = fileName + FLAG_DELETED + nowUtc.ToString(FLAG_DATEFORMAT) + FLAG_END;
                    if (File.Exists(fileName))
                        File.Move(fileName, newName);
                    else
                        File.Create(newName + ".empty"); // creates dummy file to mark as deleted
                    Interlocked.Increment(ref DeletedItems);
                }
                CheckPrintConsole(true);
                Console.WriteLine(" Delete files known in local sql but not in azure done");
            });
            AddTasks(fileDelTask, sqlDelTask);
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

            char[] jChars;
            lock (JobCharsLock)
            {
                jChars = JobChars.ToArray();
                if (jChars.Length > 0 && LastConsoleWrite < utcNow.AddSeconds(-10))
                {
                    // don't spam console to much, here we print the last Job item we dealt with
                    JobChars.RemoveWhere(jChars.Contains);
                    LastConsoleWrite = utcNow;
                    Console.Write(string.Join(string.Empty, jChars));
                }
            }

            if (forceFull || LastConsoleWriteLine < utcNow.AddMinutes(-2))
            {
                LastConsoleWriteLine = utcNow;
                // flushes every 2 minutes
                Console.WriteLine("\n --MARK-- " + utcNow.ToString("yyyy-MM-dd HH:mm:ss.ffff") + $" - Currently {ScannedItems} scanned, {TaskCount} tasks, {BlobJobQueue.QueueCount} waiting jobs");
                Console.Out.Flush();
                return true;
            }

            return jChars.Length > 0;
        }

        public async Task ProcessJob(int simultaniousDownloads)
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
            _sqlLite.Dispose();
            _sqlLite = null;
        }

        internal enum JobType
        {
            None = 0,
            New = 1,
            Modified = 2,
        }

        public class BlobJob
        {
            internal readonly BlobItem Blob;
            internal readonly string LocalFilePath;
            internal ILocalFileInfo FileInfo;
            internal FileInfoSqlite.FileInfo SqlFileInfo => FileInfo as FileInfoSqlite.FileInfo;
            internal JobType NeedsJob = JobType.None;

            private static readonly HashSet<string> HasCreatedDirectories = new HashSet<string>();

            public BlobJob(BlobItem blob, string localFilePath)
            {
                Blob = blob;
                LocalFilePath = localFilePath;
            }

            private static bool WellKnownBlob(BlobItem blob)
            {
                // Ignore empty files
                if (blob.Size == 0)
                    return true;

                // Ignore files only containing "[]"
                if (blob.Size == 2 && blob.MD5 == "11FxOYiYfpMxmANj4kGJzg==")
                    return true;

                return false;
            }

            public async Task<bool> DoJob()
            {
                try
                {
                    SqlFileInfo.UpdateFromAzure(Blob);
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
                        if (FileInfo.Size == Blob.Size &&
                            (FileInfo.MD5 != null && !string.IsNullOrEmpty(Blob.MD5) && FileInfo.MD5 == Blob.MD5))
                        {
                            // since size and hash is the same as last, we just ignore this and don't update
                            SqlFileInfo.UpdateDb();
                            if (lfi.Exists && lfi.LastWriteTimeUtc != Blob.LastModifiedUtc.UtcDateTime)
                                File.SetLastWriteTimeUtc(LocalFilePath, Blob.LastModifiedUtc.UtcDateTime);
                            return true;
                        }
                        if (lfi.Exists)
                            File.Move(LocalFilePath, LocalFilePath + FLAG_MODIFIED + Blob.LastModifiedUtc.ToString(FLAG_DATEFORMAT) + FLAG_END);
                    }
                    else
                    {
                        return true;
                    }

                    if (WellKnownBlob(Blob))
                    {
                        // no real download of these files
                        SqlFileInfo.LastDownloadedTime = DateTime.UtcNow;
                        SqlFileInfo.UpdateDb();
                        NeedsJob = JobType.None;
                        return true;
                    }

                    await Blob.DownloadToFileAsync(LocalFilePath, FileMode.Create);
                    if (lfi == null) lfi = new LocalFileInfoDisk(LocalFilePath);
                    if (lfi.Exists && lfi.LastWriteTimeUtc != Blob.LastModifiedUtc.UtcDateTime)
                        File.SetLastWriteTimeUtc(LocalFilePath, Blob.LastModifiedUtc.UtcDateTime);
                    if (lfi.Exists && string.IsNullOrEmpty(lfi.MD5))
                        lfi.CalculateMd5();
                    SqlFileInfo.UpdateFromFileInfo(lfi);
                    SqlFileInfo.UpdateDb();

                    NeedsJob = JobType.None;
                    return true;
                }
                catch (StorageException ex)
                {
                    // Swallow 404 exceptions.
                    // This will happen if the file has been deleted in the temporary period from listing blobs and downloading
                    Console.WriteLine("\nSwallowed Ex: " + LocalFilePath + " " + ex.ToString());
                }
                catch (System.IO.IOException ex)
                {
                    HasCreatedDirectories.Clear();
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
