using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;

namespace ServiceMq
{
    internal static class FastFile
    {
        internal static void Delete(string fileName)
        {
            if (Win32Utils.DeleteFile(fileName)) return;
            int lastWin32Error = Marshal.GetLastWin32Error();
            if (lastWin32Error == 2)
                return;
            throw new IOException("Delete failed", lastWin32Error);
        }

        internal static class Win32Utils
        {
            [DllImport("kernel32.dll", CharSet = CharSet.Auto, SetLastError = true, BestFitMapping = false)]
            internal static extern bool DeleteFile(string path);
        }
    }
}
