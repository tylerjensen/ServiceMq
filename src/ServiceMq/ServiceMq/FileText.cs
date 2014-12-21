using System;

namespace ServiceMq
{
    internal class FileText : IEquatable<FileText>
    {
        public string FileName { get; set; }
        public string Text { get; set; }

        public FileText() { }
        public FileText(string fileName, string text)
        {
            this.FileName = fileName;
            this.Text = text;
        }

        public override bool Equals(object obj)
        {
            var ft = obj as FileText;
            if (null == ft) return false;
            if (ft.FileName == FileName && ft.Text == Text) return true;
            return false;
        }

        public override int GetHashCode()
        {
            return base.GetHashCode();
        }

        public override string ToString()
        {
            return (FileName ?? string.Empty) 
                   + (FileName != null ? ":" : string.Empty) 
                   + (Text ?? string.Empty);
        }

        bool IEquatable<FileText>.Equals(FileText other)
        {
            return Equals(other);
        }
    }
}