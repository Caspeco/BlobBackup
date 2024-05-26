using System.Runtime.InteropServices;
using System.Text;

namespace DuplicateFinder;

public static class HardLinkHelper
{
    #region WinAPI P/Invoke declarations
    private const string Kernel32dll = "kernel32.dll";
    [DllImport(Kernel32dll, CharSet = CharSet.Unicode)]
    private static extern bool CreateHardLink(string lpFileName, string lpExistingFileName, IntPtr lpSecurityAttributes);
    
    [DllImport(Kernel32dll, CharSet = CharSet.Unicode, SetLastError = true)]
    private static extern IntPtr FindFirstFileNameW(string lpFileName, uint dwFlags, ref uint StringLength, StringBuilder LinkName);

    [DllImport(Kernel32dll, CharSet = CharSet.Unicode, SetLastError = true)]
    private static extern bool FindNextFileNameW(IntPtr hFindStream, ref uint StringLength, StringBuilder LinkName);

    [DllImport(Kernel32dll, SetLastError = true)]
    private static extern bool FindClose(IntPtr hFindFile);

    private static readonly IntPtr INVALID_HANDLE_VALUE = (IntPtr)(-1); // 0xffffffff;
    private const int MAX_PATH = 65535; // Max. NTFS path length.
    #endregion

    public static void CreateHardLink(this FileInfo src, FileInfo destination)
    {
        if (destination.Exists)
            destination.Delete();
        CreateHardLink(destination.FullName, src.FullName, IntPtr.Zero);
    }

    /// <summary>
    /// Returns the enumeration of hardlinks for the given *file* as full file paths, which includes
    /// the input path itself.
    /// </summary>
    /// <remarks>
    /// If the file has only one hardlink (itself), or you specify a directory, only that
    /// file's / directory's full path is returned.
    /// If the path refers to a volume that doesn't support hardlinks, or the path
    /// doesn't exist, empty array is returned.
    /// </remarks>
    public static string[] GetHardLinks(string filepath, int itemLimit = 0)
    {
        // Loop over and collect all hard links as their full paths.
        IntPtr findHandle = INVALID_HANDLE_VALUE;
        try
        {
            var sbPath = new StringBuilder(MAX_PATH);
            uint charCount = (uint)sbPath.Capacity; // in/out character-count variable for the WinAPI calls.

            var links = new List<string>();
            if (INVALID_HANDLE_VALUE == (findHandle = FindFirstFileNameW(filepath, 0, ref charCount, sbPath)))
                return links.ToArray();

            do
            {
                links.Add(sbPath.ToString()); // Add the full path to the result list.
                if (itemLimit != 0 && itemLimit <= links.Count)
                    break;
                charCount = (uint)sbPath.Capacity; // Prepare for the next FindNextFileNameW() call.
            } while (FindNextFileNameW(findHandle, ref charCount, sbPath));

            return links.ToArray();
        }
        finally
        {
            if (findHandle != INVALID_HANDLE_VALUE)
                FindClose(findHandle);
        }
    }

}
