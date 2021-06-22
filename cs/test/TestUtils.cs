// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using NUnit.Framework;
using System;
using System.Diagnostics;
using System.IO;

namespace FASTER.test
{
    internal static class TestUtils
    {
        internal static void DeleteDirectory(string path)
        {
            try
            {
                foreach (string directory in Directory.GetDirectories(path))
                    DeleteDirectory(directory);
            }
            catch (DirectoryNotFoundException)
            {
                // Ignore this; some tests call this before the test run to make sure there are no leftovers (e.g. from a debug session).
            }

            // Exceptions may happen due to a handle briefly remaining held after Dispose().
            try
            {
                Directory.Delete(path, true);
            }
            catch (Exception ex) when (ex is IOException ||
                                       ex is UnauthorizedAccessException)
            {
                try
                {
                    Directory.Delete(path, true);
                }
                catch { }
            }
        }

        private static string ConvertedClassName(bool forAzure = false)
        {
            // Make this all under one root folder named {prefix}, which is the base namespace name. All UT namespaces using this must start with this prefix.
            const string prefix = "FASTER.test";
            Debug.Assert(TestContext.CurrentContext.Test.ClassName.StartsWith($"{prefix}."));
            return $"{prefix}{(forAzure ? '-' : '/')}{TestContext.CurrentContext.Test.ClassName.Substring(prefix.Length + 1)}";
        }

        internal static string ClassTestDir => Path.Combine(TestContext.CurrentContext.TestDirectory, ConvertedClassName());

        internal static string MethodTestDir => Path.Combine(ClassTestDir, TestContext.CurrentContext.Test.MethodName);

        internal static string AzureMethodTestContainer => $"{ConvertedClassName(forAzure: true)}-{TestContext.CurrentContext.Test.MethodName}".Replace('.', '-').ToLower();

        internal const string AzureEmulatedStorageString = "UseDevelopmentStorage=true;";
    }
}
