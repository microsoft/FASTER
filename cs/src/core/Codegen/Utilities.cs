// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using System;
using System.Collections.Generic;
using System.Diagnostics.Contracts;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Reflection.Emit;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

namespace FASTER.core.Roslyn
{

    internal static class Helper
    {
        public static bool IsAnonymousTypeName(this Type type)
        {
            Contract.Requires(type != null);

            return type.GetTypeInfo().IsClass
                && type.GetTypeInfo().IsDefined(typeof(CompilerGeneratedAttribute))
                && !type.IsNested
                && type.Name.StartsWith("<>", StringComparison.Ordinal)
                && type.Name.Contains("__Anonymous");
        }
        /// <summary>
        /// Returns true if <paramref name="type"/> is an anonymous type or is a generic type
        /// with an anonymous type somewhere in the type tree(s) of its type arguments.
        /// REVIEW: Is there a better way to tell if a type represents an anonymous type?
        /// </summary>
        public static bool IsAnonymousType(this Type type)
        {
            Contract.Requires(type != null);

            if (type.IsAnonymousTypeName() || type.GetTypeInfo().Assembly.IsDynamic) return true;
            if (!type.GetTypeInfo().IsGenericType)
                return false;
            else
                return type.GenericTypeArguments.Any(t => t.IsAnonymousType());
        }

        public static string GetCSharpSourceSyntax(this Type t)
        {
            var list = new List<string>();
            string ret = TurnTypeIntoCSharpSource(t, ref list);
            return ret;
        }
        private static string TurnTypeIntoCSharpSource(Type t, ref List<string> introducedGenericTypeParameters)
        {
            Contract.Requires(t != null);
            Contract.Requires(introducedGenericTypeParameters != null);

            var typeName = t.FullName.Replace('#', '_').Replace('+', '.');
            if (t.IsAnonymousTypeName())
            {
                var newGenericTypeParameter = t.FullName.CleanUpIdentifierName(); // "A" + introducedGenericTypeParameters.Count.ToString(CultureInfo.InvariantCulture);
                introducedGenericTypeParameters.Add(newGenericTypeParameter);
                return newGenericTypeParameter;
            }
            if (!t.GetTypeInfo().IsGenericType) // need to test after anonymous because deserialized anonymous types are *not* generic (but unserialized anonymous types *are* generic)
                return typeName;
            var sb = new StringBuilder();
            typeName = typeName.Substring(0, t.FullName.IndexOf('`'));
            sb.AppendFormat("{0}<", typeName);
            var first = true;
            if (!t.GetTypeInfo().Assembly.IsDynamic)
                foreach (var genericArgument in t.GenericTypeArguments)
                {
                    string gaName = TurnTypeIntoCSharpSource(genericArgument, ref introducedGenericTypeParameters);
                    if (!first) sb.Append(", ");
                    sb.Append(gaName);
                    first = false;
                }
            sb.Append(">");
            typeName = sb.ToString();
            return typeName;
        }
        public static string CleanUpIdentifierName(this string s)
        {
            Contract.Requires(s != null);

            return s.Replace('`', '_').Replace('.', '_').Replace('<', '_').Replace('>', '_').Replace(',', '_').Replace(' ', '_').Replace('`', '_').Replace('[', '_').Replace(']', '_').Replace('=', '_').Replace('+', '_');
        }

        internal static bool IsBlittable<T>()
        {
            if (default(T) == null)
                return false;

            //if (typeof(T).IsGenericType) return false;

            try
            {
                var tmp = new T[1];
                var h = GCHandle.Alloc(tmp, GCHandleType.Pinned);
                h.Free();
            }
            catch (Exception)
            {
                return false;
            }
            return true;
        }

        internal static string GeneratedDirectory
        {
            get
            {
                if (generatedDirectory == null)
                {
                    var tempPath = Path.GetTempPath();
                    generatedDirectory = Path.Combine(tempPath, "FASTER");
                    try
                    {
                        if (!Directory.Exists(generatedDirectory))
                        {
                            Directory.CreateDirectory(generatedDirectory);
                        }
                    }
                    catch
                    {
                        Directory.CreateDirectory("Generated"); // let any exceptions bleed through
                        generatedDirectory = "Generated";
                    }
                }
                return generatedDirectory;
            }
        }
        private static string generatedDirectory;
    }
    internal class TypeMapper
    {
        Dictionary<Type, string> typeMap = new Dictionary<Type, string>();
        int anonymousTypeCount = 0;

        public TypeMapper(params Type[] types)
        {
            GetCSharpTypeNames(types);
        }

        public string CSharpNameFor(Type t)
        {
            if (!typeMap.TryGetValue(t, out string typeName))
            {
                GetCSharpTypeNames(t);
                typeName = typeMap[t];
            }
            return typeName;
        }

        public IEnumerable<string> GenericTypeVariables(params Type[] types)
        {
            var l = new List<string>();
            foreach (var t in types)
            {
                if (t.IsAnonymousTypeName())
                {
                    l.Add(typeMap[t]);
                    continue;
                }
                if (!t.GetTypeInfo().Assembly.IsDynamic && t.GetTypeInfo().IsGenericType)
                {
                    foreach (var gta in t.GenericTypeArguments)
                    {
                        l.AddRange(GenericTypeVariables(gta));
                    }
                }
            }
            return l.Distinct();
        }

        private void GetCSharpTypeNames(params Type[] types)
        {
            for (int i = 0; i < types.Length; i++)
            {
                TurnTypeIntoCSharpSourceHelper(types[i]);
            }
            return;
        }

        private void TurnTypeIntoCSharpSourceHelper(Type t)
        {
            Contract.Requires(t != null);

            if (this.typeMap.TryGetValue(t, out string typeName))
            {
                return;
            }
            typeName = t.FullName.Replace('#', '_').Replace('+', '.');
            if (t.IsAnonymousTypeName())
            {
                var newGenericTypeParameter = "A" + anonymousTypeCount.ToString(CultureInfo.InvariantCulture);
                anonymousTypeCount++;
                this.typeMap.Add(t, newGenericTypeParameter);
                return;
            }
            if (!t.GetTypeInfo().IsGenericType) // need to test after anonymous because deserialized anonymous types are *not* generic (but unserialized anonymous types *are* generic)
            {
                this.typeMap.Add(t, typeName);
                return;
            }
            var sb = new StringBuilder();
            typeName = typeName.Substring(0, t.FullName.IndexOf('`'));
            sb.AppendFormat("{0}<", typeName);
            var first = true;
            if (!t.GetTypeInfo().Assembly.IsDynamic)
            {
                foreach (var genericArgument in t.GenericTypeArguments)
                {
                    TurnTypeIntoCSharpSourceHelper(genericArgument);
                    string gaName = this.typeMap[genericArgument];
                    if (!first) sb.Append(", ");
                    sb.Append(gaName);
                    first = false;
                }
            }
            sb.Append(">");
            typeName = sb.ToString();
            this.typeMap.Add(t, typeName);
            return;
        }
    }

    public static class TypeSize
    {
        public static unsafe int GetSize<T>(this T value)
        {
            T[] arr = new T[2];
            return (int)((long)Unsafe.AsPointer(ref arr[1]) - (long)Unsafe.AsPointer(ref arr[0]));
        }
    }
}
