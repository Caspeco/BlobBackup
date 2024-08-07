﻿
namespace BlobBackup
{
    public class RunQueue<T> : IDisposable
    {
        private readonly object _doneQueueLock = new();
        private bool _noMoreAddsToBeDone;
        private readonly AutoResetEvent _doneReset = new(false);
        private readonly Queue<T> _doneQueue = new();

        private int _doneCount;
        /// <summary>Total times <see cref="AddDone"/> has been called</summary>
        public int TotalDoneCount => _doneCount;

        private T _lastDoneItem;
        /// <summary>The item <see cref="AddDone"/> was last called with</summary>
        public T LastDoneItem => _lastDoneItem;

        public bool Complete => _noMoreAddsToBeDone;

        /// <exception cref="InvalidOperationException">If <see cref="RunnerDone" /> has been called</exception>
        public void AddDone(T runI)
        {
            if (_noMoreAddsToBeDone)
                throw new InvalidOperationException("Can not call AddDone after RunnerDone already called");

            lock (_doneQueueLock)
            {
                _doneQueue.Enqueue(runI);
                Interlocked.Increment(ref _doneCount);
                _lastDoneItem = runI;
            }
            _doneReset.Set();
        }

        public int QueueCount => _doneQueue.Count;

        private T _GetNextDone()
        {
            //  yield can not be used inside trycatch or synclock
            lock (_doneQueueLock)
            {
                if (QueueCount == 0)
                    return default;
                return _doneQueue.Dequeue();
            }
        }

        public IEnumerable<T> GetDoneEnumerable()
        {
            // helper function to get a enumerator since yield can't be used inside synclock or trycatch
            if (!_noMoreAddsToBeDone)
                _doneReset.WaitOne();

            while (true)
            {
                var runI = _GetNextDone();
                if (runI is null)
                {
                    // Make sure we run until final notice
                    if (!_noMoreAddsToBeDone)
                    {
                        _doneReset.WaitOne();
                    }
                    else
                    {
                        // wait a bit if done just in case
                        _doneReset.WaitOne(10 * 1000);
                    }

                    if (_noMoreAddsToBeDone && QueueCount == 0)
                        break;

                    continue;
                }
                yield return runI;
            }
            _doneReset.Set(); // ensure that any remaining threads are let go
        }

        /// <exception cref="InvalidOperationException">If <see cref="RunnerDone" /> has been called before</exception>
        internal void RunnerDone()
        {
            if (_noMoreAddsToBeDone)
                throw new InvalidOperationException("RunnerDone already called");
            // notify done
            _noMoreAddsToBeDone = true;
            _doneReset.Set();
        }

        #region IDisposable Support
        private bool disposedValue = false; // To detect redundant calls

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    _doneReset.Dispose();
                }

                disposedValue = true;
            }
        }

        // This code added to correctly implement the disposable pattern.
        public void Dispose()
        {
            Dispose(true);
        }
        #endregion
    }
}
