using System;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;

namespace ServiceMq
{
    public sealed class AesStorageProtector : IStorageProtector
    {
        private const string Prefix = "smq1:";
        private readonly byte[] encryptionKey;
        private readonly byte[] authenticationKey;

        public AesStorageProtector(byte[] key)
        {
            if (key == null || (key.Length != 16 && key.Length != 24 && key.Length != 32))
                throw new ArgumentException("The AES key must be 16, 24, or 32 bytes.", "key");
            encryptionKey = (byte[])key.Clone();
            using (var sha = SHA256.Create())
                authenticationKey = sha.ComputeHash(key.Concat(Encoding.UTF8.GetBytes("ServiceMq authentication")).ToArray());
        }

        public string Protect(string value)
        {
            using (var aes = Aes.Create())
            {
                aes.Key = encryptionKey;
                aes.GenerateIV();
                byte[] cipher;
                using (var output = new MemoryStream())
                using (var crypto = new CryptoStream(output, aes.CreateEncryptor(), CryptoStreamMode.Write))
                using (var writer = new StreamWriter(crypto, new UTF8Encoding(false)))
                {
                    writer.Write(value ?? string.Empty);
                    writer.Flush();
                    crypto.FlushFinalBlock();
                    cipher = output.ToArray();
                }
                var authenticated = aes.IV.Concat(cipher).ToArray();
                byte[] mac;
                using (var hmac = new HMACSHA256(authenticationKey)) mac = hmac.ComputeHash(authenticated);
                return Prefix + Convert.ToBase64String(authenticated.Concat(mac).ToArray());
            }
        }

        public string Unprotect(string value)
        {
            if (value == null || !value.StartsWith(Prefix, StringComparison.Ordinal)) return value;
            var data = Convert.FromBase64String(value.Substring(Prefix.Length));
            if (data.Length < 49) throw new CryptographicException("Invalid ServiceMq encrypted payload.");
            var authenticatedLength = data.Length - 32;
            var authenticated = data.Take(authenticatedLength).ToArray();
            var suppliedMac = data.Skip(authenticatedLength).ToArray();
            byte[] expectedMac;
            using (var hmac = new HMACSHA256(authenticationKey)) expectedMac = hmac.ComputeHash(authenticated);
            if (!FixedTimeEquals(suppliedMac, expectedMac)) throw new CryptographicException("ServiceMq encrypted payload authentication failed.");
            var iv = authenticated.Take(16).ToArray();
            var cipher = authenticated.Skip(16).ToArray();
            using (var aes = Aes.Create())
            {
                aes.Key = encryptionKey;
                aes.IV = iv;
                using (var input = new MemoryStream(cipher))
                using (var crypto = new CryptoStream(input, aes.CreateDecryptor(), CryptoStreamMode.Read))
                using (var reader = new StreamReader(crypto, Encoding.UTF8)) return reader.ReadToEnd();
            }
        }

        private static bool FixedTimeEquals(byte[] left, byte[] right)
        {
            if (left.Length != right.Length) return false;
            var difference = 0;
            for (var i = 0; i < left.Length; i++) difference |= left[i] ^ right[i];
            return difference == 0;
        }
    }
}
