using System.ComponentModel;
using System.Runtime.InteropServices;

namespace DuplicateFinder;

public static partial class HardLinkHelper
{
    #region WinAPI P/Invoke declarations
    private const string Kernel32dll = "kernel32.dll";
    [LibraryImport(Kernel32dll, EntryPoint = "CreateHardLinkW", StringMarshalling = StringMarshalling.Utf16, SetLastError = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static partial bool CreateHardLink(string lpFileName, string lpExistingFileName, IntPtr lpSecurityAttributes);

    [LibraryImport(Kernel32dll, EntryPoint = "FindFirstFileNameW", StringMarshalling = StringMarshalling.Utf16, SetLastError = true)]
    private static partial IntPtr FindFirstFileName(string lpFileName, uint dwFlags, ref uint StringLength, char[] LinkName);

    [LibraryImport(Kernel32dll, EntryPoint = "FindNextFileNameW", StringMarshalling = StringMarshalling.Utf16, SetLastError = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static partial bool FindNextFileName(IntPtr hFindStream, ref uint StringLength, char[] LinkName);

    [LibraryImport(Kernel32dll, EntryPoint = "FindClose", SetLastError = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static partial bool FindClose(IntPtr hFindFile);

    private static readonly IntPtr INVALID_HANDLE_VALUE = -1; // 0xffffffff;
    private const int MAX_PATH = 65535; // Max. NTFS path length.
    #endregion

    /// <param name="destination">Location to create a link at, that will have the same contents as <paramref name="src"/></param>
    public static bool CreateHardLink(this FileInfo src, FileInfo destination, bool doReplace = true, bool reuseLink = false, bool dothrow = false)
    {
        if (!src.Exists)
        {
            throw new FileNotFoundException("HardLink source not found", src.FullName);
        }
        var srcRoot = Path.GetPathRoot(src.FullName) ?? string.Empty;
        if (srcRoot != Path.GetPathRoot(destination.FullName))
        {
            if (dothrow) throw new IOException("Source and destination must be on same drive for HardLink");
            return false;
        }
        if (reuseLink && destination.Exists && destination.EnumerateHardLinks().Contains(src.FullName.Remove(0, srcRoot.Length - 1)))
            return true;
        if (doReplace && destination.Exists)
            destination.Delete();
        var ret = CreateHardLink(destination.FullName, src.FullName, IntPtr.Zero);
        if (dothrow && !ret)
        {
            throw new Win32Exception();
        }
        return ret;
    }

    /// <summary>
    /// Returns the enumeration of hardlinks for the given *file* as full file paths, which includes
    /// the input path itself.
    /// </summary>
    /// <remarks>
    /// If the file has only one hardlink (itself), or you specify a directory, only that full path is returned.
    /// Path that refers to volume without hardlink support gives empty result.
    /// Volume identifier is not present in paths
    /// </remarks>
    public static IEnumerable<string> EnumerateHardLinks(this FileInfo fi)
    {
        // Loop over and collect all hard links as their full paths.
        IntPtr findHandle = INVALID_HANDLE_VALUE;
        try
        {
            var sbPath = new char[MAX_PATH + 1];
            uint charCount = MAX_PATH; // in/out character-count variable for the WinAPI calls.

            if (INVALID_HANDLE_VALUE == (findHandle = FindFirstFileName(fi.FullName, 0, ref charCount, sbPath)))
                yield break;

            do
            {
                yield return new string(sbPath[.. (((int)charCount) - 1)]).Trim('\0');
                charCount = MAX_PATH; // reset for next FindNextFileNameW() call
            } while (FindNextFileName(findHandle, ref charCount, sbPath));
        }
        finally
        {
            if (findHandle != INVALID_HANDLE_VALUE)
                FindClose(findHandle);
        }
    }

}
