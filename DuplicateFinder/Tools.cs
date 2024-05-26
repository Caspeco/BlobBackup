namespace DuplicateFinder;

public static class Tools
{
    public static ParallelQuery<FileInfo> EnumerateFilesParallel(DirectoryInfo dir) =>
        dir.EnumerateDirectories()
            .SelectMany(EnumerateFilesParallel)
            .Concat(dir.EnumerateFiles("*", SearchOption.TopDirectoryOnly))
            .AsParallel();

    /// <summary>
    /// Gets key in dic. If not present, returns or.
    /// </summary>
    public static TValue? Get<TKey, TValue>(this IDictionary<TKey, TValue> dic, TKey key, TValue? or = default) =>
        dic.TryGetValue(key, out TValue? value)
            ? value
            : or;

    /// <summary>
    /// Gets key in dic. If not present, creates new instance of TValue and adds to Dictionary before returning.
    /// </summary>
    public static TValue GetOrNew<TKey, TValue>(this IDictionary<TKey, TValue> dic, TKey key) where TValue : new()
    {
        if (!dic.TryGetValue(key, out TValue? value))
            dic.Add(key, value = new TValue());
        return value;
    }
}
