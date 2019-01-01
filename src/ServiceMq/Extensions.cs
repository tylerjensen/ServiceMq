using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace ServiceMq
{
    internal static class Extensions
    {
        private const string CR = "\r";
        private const string LF = "\n";
        private const string CREsc = "\\r";
        private const string LFEsc = "\\n";

        internal static string ToFlatLine(this string line)
        {
            return line.Replace(CR, CREsc).Replace(LF, LFEsc);
        }

        internal static string FromFlatLine(this string line)
        {
            return line.Replace(CREsc, CR).Replace(LFEsc, LF);
        }
    }
}
