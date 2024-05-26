
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace DuplicateFinder;

public static class Tools
{
    public static ParallelQuery<FileInfo> EnumerateFilesParallel(DirectoryInfo dir)
    {
        return dir.EnumerateDirectories()
            .SelectMany(EnumerateFilesParallel)
            .Concat(dir.EnumerateFiles("*", SearchOption.TopDirectoryOnly))
            .AsParallel();
    }

    /// <summary>
    /// Gets key in dic. If not present, returns or.
    /// </summary>
    public static TValue Get<TKey, TValue>(this IDictionary<TKey, TValue> dic, TKey key, TValue or = default(TValue))
    {
        TValue value;
        return dic.TryGetValue(key, out value)
            ? value
            : or;
    }
    
    /// <summary>
    /// Gets key in dic. If not present, creates new instance of TValue and adds to Dictionary before returning.
    /// </summary>
    public static TValue GetOrNew<TKey, TValue>(this IDictionary<TKey, TValue> dic, TKey key) where TValue : new()
    {
        TValue value;
        if (!dic.TryGetValue(key, out value))
            dic.Add(key, value = new TValue());
        return value;
    }
}
