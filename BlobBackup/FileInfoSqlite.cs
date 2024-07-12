using System.Data.SQLite;

namespace BlobBackup
{
    public class FileInfoSqlite : IDisposable
    {
        private readonly string _sqlLitePath;
        private readonly ReaderWriterLockSlim _readerWriterLock = new();
        private readonly SQLiteConnection dbConnection;

        private const string SQL_TABLENAME = "files";

        public FileInfoSqlite(string containerName, string sqlLitePath)
        {
            _sqlLitePath = sqlLitePath;

            Directory.CreateDirectory(_sqlLitePath);
            var sqlFile = Path.Combine(_sqlLitePath, containerName + ".sqlite");
            var sqlFileMissing = !File.Exists(sqlFile);
            if (sqlFileMissing) SQLiteConnection.CreateFile(sqlFile);

            dbConnection = new SQLiteConnection($"Data Source={sqlFile};Version=3;cache=shared;Pooling=True");
            dbConnection.Open();

            // note that column types are specified for clearity rather than what SQLite uses internally
            ExecuteNonQuery(
                "CREATE TABLE IF NOT EXISTS " + SQL_TABLENAME + " (" +
                " LocalName VARCHAR(1024) NOT NULL PRIMARY KEY," +
                " RemPath VARCHAR(1024) NOT NULL," +
                " LastModifiedTime DATETIME NOT NULL," +
                " Size INT NOT NULL," +
                " AzureMD5 VARCHAR(32) NOT NULL," +
                " LastDownloadedTime DATETIME NULL," +
                " DeleteDetectedTime DATETIME NULL" +
                ");");

            ExecuteNonQuery("PRAGMA read_uncommitted = true"); // speed up when shared, internally sqlite won't use mutex locks
        }

        private SQLiteCommand GetCmd(string sql, object[] parameters)
        {
            var cmd = new SQLiteCommand(sql, dbConnection);
            if (parameters is not null)
            {
                int paramNumber = 1;
                foreach (object value in parameters)
                {
                    cmd.Parameters.AddWithValue(paramNumber.ToString(), value);
                    paramNumber++;
                }
            }

            return cmd;
        }

        /// <remarks>This uses locks internally so should be safe to call</remarks>
        private int ExecuteNonQuery(string sql, params object[] parameters)
        {
            try
            {
                _readerWriterLock.EnterWriteLock();

                using var cmd = GetCmd(sql, parameters);
                return cmd.ExecuteNonQuery();
            }
            finally
            {
                _readerWriterLock.ExitWriteLock();
            }
        }

        /// <remarks>MUST be locked from outside</remarks>
        private SQLiteDataReader ExecuteReader(string sql, params object[] parameters)
        {
            try
            {
                _readerWriterLock.EnterReadLock();

                using var cmd = GetCmd(sql, parameters);
                return cmd.ExecuteReader();
            }
            finally
            {
                _readerWriterLock.ExitReadLock();
            }
        }

        private static DateTime? GetDateTime(object value)
        {
            if (value is null || value == DBNull.Value)
                return null;
            return Convert.ToDateTime(value);
        }

        private const string SQL_BEGINTRANSACTION = "BEGIN TRANSACTION";
        private const string SQL_ENDTRANSACTION = "END TRANSACTION";

        private bool inTransaction;

        public bool BeginTransaction()
        {
            if (inTransaction)
                return false;
            ExecuteNonQuery(SQL_BEGINTRANSACTION);
            inTransaction = true;
            return true;
        }

        public bool EndTransaction()
        {
            if (!inTransaction)
                return false;
            ExecuteNonQuery(SQL_ENDTRANSACTION);
            inTransaction = false;
            return true;
        }

        internal FileInfo GetFileInfo(ILocalFileInfo lfi, string LocalName = null)
        {
            using var reader = ExecuteReader("SELECT * FROM " + SQL_TABLENAME + " WHERE LocalName=@1", LocalName);
            if (reader.Read())
            {
                return new FileInfo(this, lfi, reader);
            }
            return null;
        }

        internal IEnumerable<FileInfo> GetAllFileInfos()
        {
            using var reader = ExecuteReader("SELECT * FROM " + SQL_TABLENAME + " WHERE DeleteDetectedTime IS NULL");
            while (reader.Read())
            {
                yield return new FileInfo(this, null, reader);
            }
        }

        public ILocalFileInfo GetFileInfo(BlobItem blob, System.IO.FileInfo localFile = null)
        {
            ILocalFileInfo lfi = localFile is null ? null : new LocalFileInfoDisk(localFile);
            var fi = GetFileInfo(lfi, blob.GetLocalFileName());
            if (fi is not null)
                return fi;

            // create new instance
            ((LocalFileInfoDisk)lfi)?.GetMd5();
            fi = new FileInfo(this, lfi, blob);

            // make sure we insert item when there is nothing found
            ExecuteNonQuery("INSERT INTO " + SQL_TABLENAME +
                " (LocalName, RemPath, LastModifiedTime, Size, AzureMD5, LastDownloadedTime, DeleteDetectedTime)" +
                " VALUES (@1, @2, @3, @4, @5, @6, @7)",
                fi.LocalName, fi.RemPath, fi.LastModifiedTime, fi.Size, fi.MD5, fi.LastDownloadedTime, fi.DeleteDetectedTime);

            return fi;
        }

        private void UpdateFileInfo(FileInfo fi)
        {
            ExecuteNonQuery("UPDATE " + SQL_TABLENAME +
                " SET RemPath=@2, LastModifiedTime=@3, Size=@4, AzureMD5=@5, LastDownloadedTime=@6, DeleteDetectedTime=@7" +
                " WHERE LocalName=@1",
                fi.LocalName, fi.RemPath, fi.LastModifiedTime, fi.Size, fi.MD5, fi.LastDownloadedTime, fi.DeleteDetectedTime);
        }

        public class FileInfo : ILocalFileInfo
        {
            private readonly FileInfoSqlite _sqlLite;
            public readonly ILocalFileInfo SrcFileInfo;

            public bool Exists => LastDownloadedTime.HasValue;
            public long Size { get; set; }
            public string MD5 { get; set; }
            public DateTime LastModifiedTimeUtc => LastModifiedTime.ToUniversalTime();

            public string LocalName { get; private set; }
            public string RemPath { get; private set; }
            public DateTime LastModifiedTime { get; private set; }
            public DateTime? LastDownloadedTime { get; set; }
            public DateTime? DeleteDetectedTime { get; set; }

            private FileInfo(FileInfoSqlite sqlite)
            {
                _sqlLite = sqlite;
            }

            internal FileInfo(FileInfoSqlite sqlite, ILocalFileInfo fi, SQLiteDataReader reader)
                : this(sqlite)
            {
                SrcFileInfo = fi;
                LocalName = Convert.ToString(reader["LocalName"]);
                RemPath = Convert.ToString(reader["RemPath"]);
                LastModifiedTime = Convert.ToDateTime(reader["LastModifiedTime"]);
                Size = Convert.ToInt64(reader["Size"]);
                MD5 = Convert.ToString(reader["AzureMD5"]);
                LastDownloadedTime = GetDateTime(reader["LastDownloadedTime"]);
                DeleteDetectedTime = GetDateTime(reader["DeleteDetectedTime"]);

                // if there is local file, than it takes precedence over database values
                if (UpdateFromFileInfo(fi))
                    UpdateDb(); // if the local file updated object then reflect that in db
            }

            private FileInfo(FileInfoSqlite sqlite, BlobItem blob)
                : this(sqlite)
            {
                SrcFileInfo = blob;
                UpdateFromAzure(blob);
            }

            internal void UpdateFromAzure(BlobItem blob)
            {
                LocalName = blob.GetLocalFileName();
                RemPath = blob.Name;
                LastModifiedTime = blob.LastModifiedUtc.UtcDateTime;
                Size = blob.Size;
                MD5 = blob.MD5;
            }

            internal FileInfo(FileInfoSqlite sqlite, ILocalFileInfo fi, BlobItem blob)
                : this(sqlite, blob)
            {
                SrcFileInfo = fi;
                UpdateFromFileInfo(fi);
            }

            internal bool UpdateFromFileInfo(ILocalFileInfo fi)
            {
                if (fi is null || !fi.Exists)
                    return false;

                if (string.IsNullOrEmpty(fi.MD5) &&
                    fi is LocalFileInfoDisk lfi) MD5 = lfi.GetMd5();
                var same = fi.IsSame(this);

                LastModifiedTime = fi.LastModifiedTimeUtc;
                Size = fi.Size;
                if (!string.IsNullOrEmpty(fi.MD5))
                    MD5 = fi.MD5;

                return !same;
            }

            internal void UpdateDb() => _sqlLite.UpdateFileInfo(this);

            public override string ToString() => string.Join("|", RemPath, Size, LastModifiedTime, MD5, LastDownloadedTime, DeleteDetectedTime);
        }


        #region IDisposable Support
        private bool disposedValue = false; // To detect redundant calls

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                EndTransaction();
                dbConnection.Close();
                if (disposing)
                {
                    // dispose managed state (managed objects).
                    dbConnection.Dispose();
                }

                // free unmanaged resources (unmanaged objects) and override a finalizer below.
                // set large fields to null.

                disposedValue = true;
            }
        }

        ~FileInfoSqlite()
        {
            // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
            Dispose(false);
        }

        // This code added to correctly implement the disposable pattern.
        public void Dispose()
        {
            // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
            Dispose(true);
            GC.SuppressFinalize(this);
        }
        #endregion
    }
}
