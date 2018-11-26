// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using static FASTER.core.Roslyn.Helper;

namespace FASTER.core.Roslyn
{
    class CompilerBase
    {
        protected CSharpCompilation compilation;
        protected Dictionary<string, MetadataReference> metadataReferences = new Dictionary<string, MetadataReference>();
        protected IEnumerable<Template> Sources;
        private CSharpParseOptions parseOptions;

        public CompilerBase(IEnumerable<string> templateNames)
        {
            var sources = new List<Template>();
            foreach (var tN in templateNames)
            {
                sources.Add(new Template(tN));
            }
            this.Sources = sources;
        }

#if DOTNETCORE
        internal static IEnumerable<PortableExecutableReference> NetCoreAssemblyReferences
        {
            get
            {
                if (netCoreAssemblyReferences == null)
                {
                    // According to https://docs.microsoft.com/en-us/dotnet/core/tutorials/netcore-hosting,
                    // result of TRUSTED_PLATFORM_ASSEMBLIES is separated by ';' in windows, ':' in Linux(and Mac?)
                    var pathEnvSeparator = Environment.OSVersion.Platform == PlatformID.Win32NT ? ';' : ':';
                    var allAvailableAssemblies = ((string)AppContext.GetData("TRUSTED_PLATFORM_ASSEMBLIES")).Split(pathEnvSeparator);

                    // From: http://source.roslyn.io/#Microsoft.CodeAnalysis.Scripting/ScriptOptions.cs,40
                    // These references are resolved lazily. Keep in sync with list in core csi.rsp.
                    var files = new[]
                    {
                        "System.Collections",
                        "System.Collections.Concurrent",
                        "System.Console",
                        "System.Diagnostics.Debug",
                        "System.Diagnostics.Process",
                        "System.Diagnostics.StackTrace",
                        "System.Diagnostics.TraceSource",
                        "System.Globalization",
                        "System.IO",
                        "System.IO.FileSystem",
                        "System.IO.FileSystem.Primitives",
                        "System.Reflection",
                        "System.Reflection.Extensions",
                        "System.Reflection.Primitives",
                        "System.Runtime",
                        "System.Runtime.CompilerServices.Unsafe", // added 6/11/2018
                        "System.Runtime.Extensions",
                        "System.Runtime.InteropServices",
                        "System.Text.Encoding",
                        "System.Text.Encoding.CodePages",
                        "System.Text.Encoding.Extensions",
                        "System.Text.RegularExpressions",
                        "System.Threading",
                        "System.Threading.Overlapped",
                        "System.Threading.Tasks",
                        "System.Threading.Tasks.Parallel",
                        "System.Threading.Thread",
                    };
                    var filteredPaths = allAvailableAssemblies.Where(p => files.Concat(new string[] { "mscorlib", "netstandard", "System.Private.CoreLib", "System.Runtime.Serialization.Primitives", }).Any(f => Path.GetFileNameWithoutExtension(p).Equals(f)));
                    netCoreAssemblyReferences = filteredPaths.Select(p => MetadataReference.CreateFromFile(p));
                }
                return netCoreAssemblyReferences;
            }
        }
        private static IEnumerable<PortableExecutableReference> netCoreAssemblyReferences;
#endif

        protected void CreateCompilation(bool persistGeneratedCode, bool optimizeCode, IEnumerable<string> preprocessorSymbols = null)
        {
            var assemblyName = Path.GetFileNameWithoutExtension(Path.GetRandomFileName());
            var assemblyDirectory = Path.Combine(GeneratedDirectory, assemblyName);

            if (persistGeneratedCode)
            {
                if (Directory.Exists(assemblyDirectory))
                {
                    Directory.Delete(assemblyDirectory);
                }
                Directory.CreateDirectory(assemblyDirectory);
            }


            IEnumerable<MetadataReference> references;


#if DOTNETCORE
            references = NetCoreAssemblyReferences;
#else
            MetadataReference mscorlib = MetadataReference.CreateFromFile(typeof(object).GetTypeInfo().Assembly.Location);
            MetadataReference system = MetadataReference.CreateFromFile(typeof(Queue<>).GetTypeInfo().Assembly.Location);
            MetadataReference linq = MetadataReference.CreateFromFile(typeof(System.Linq.Enumerable).GetTypeInfo().Assembly.Location);
            MetadataReference contracts = MetadataReference.CreateFromFile(typeof(System.Runtime.Serialization.DataContractAttribute).GetTypeInfo().Assembly.Location);
            var thisAssembly = MetadataReference.CreateFromFile(this.GetType().GetTypeInfo().Assembly.Location);

            MetadataReference unsaferef = MetadataReference.CreateFromFile(typeof(Unsafe).GetTypeInfo().Assembly.Location);
            // The Unsafe assembly depends (at compile time) on System.Runtime.dll
            // Since that assembly does not define any types (just type forwarders), I don't know how to get
            // a reference to it without just looking into the file system.
            var systemRuntimeLocation = $"{System.Environment.GetEnvironmentVariable("windir")}\\Microsoft.NET\\assembly\\GAC_MSIL\\System.Runtime\\v4.0_4.0.0.0__b03f5f7f11d50a3a\\System.Runtime.dll";
            var systemRuntimeRef = MetadataReference.CreateFromFile(systemRuntimeLocation);

            references = new List<MetadataReference>(){ mscorlib, system, linq, contracts, thisAssembly, unsaferef, systemRuntimeRef, };
#endif
            foreach (var r in references)
            {
                this.metadataReferences.Add(r.Display, r);
            }

            CSharpCompilationOptions options = new CSharpCompilationOptions(
                allowUnsafe: true,
                optimizationLevel: (optimizeCode ? OptimizationLevel.Release : OptimizationLevel.Debug),
                outputKind: OutputKind.DynamicallyLinkedLibrary
                );

            var preprocessorSyms = new List<string>() {  };
            if (preprocessorSymbols != null)
            {
                preprocessorSyms.AddRange(preprocessorSymbols);
            }

            this.parseOptions = new CSharpParseOptions().WithPreprocessorSymbols(preprocessorSyms);
            var trees = Sources
                .Select(s => CSharpSyntaxTree.ParseText(
                s.TemplateContents,
                path: Path.Combine(assemblyDirectory, s.TemplateName + ".cs"),
                encoding: Encoding.GetEncoding(0),
                options: parseOptions
            ));
            compilation = CSharpCompilation.Create(assemblyName, trees, references, options);

        }

        public void AddAssemblyReferencesNeededFor(params Type[] args)
        {
            var rs = args
                .SelectMany(a => RoslynHelpers.MetadataReferencesNeededForType(a))
                .Where(r => !this.metadataReferences.ContainsKey(r.Display))
                .Where(r => !this.compilation.References.Any(r2 => r2.Display.Equals(r.Display)))
                ;
            compilation = compilation.AddReferences(rs);
            //addAssemblyReferences(Microsoft.StreamProcessing.CommonTransformer.Transformer.AssemblyReferencesNeededFor(args).ToArray());
        }
        public void AddSource(string source, string fileName)
        {
            var assemblyDirectory = Path.GetDirectoryName(compilation.SyntaxTrees.First().FilePath);
            var tree = CSharpSyntaxTree.ParseText(
                source,
                path: Path.Combine(assemblyDirectory, fileName + ".cs"),
                encoding: Encoding.GetEncoding(0),
                options: parseOptions
            );
            this.compilation = this.compilation.AddSyntaxTrees(tree);
        }
        public Tuple<Assembly, string> Compile(bool persistGeneratedCode)
        {
            string errors = "";

            try
            {
                if (persistGeneratedCode)
                {
                    foreach (var t in this.compilation.SyntaxTrees)
                    {
                        var root = t.GetRoot();
                        ReplaceNode(root, root.NormalizeWhitespace()); // updates compilation

                    }
                }
                var assembly = RoslynHelpers.EmitCompilationAndLoadAssembly(compilation, persistGeneratedCode, out errors);
                return Tuple.Create(assembly, errors);
            }
            catch (Exception)
            {
                RoslynHelpers.DebugPPrint(compilation.SyntaxTrees[0].GetRoot()); // BUG: Need to print entire compilation?
                System.Diagnostics.Debug.WriteLine("Can't load dll for the above code");
                System.Diagnostics.Debug.WriteLine(errors);
                throw;
            }
        }
        public void ReplaceNode(SyntaxNode oldNode, SyntaxNode newNode)
        {
            var oldTree = oldNode.SyntaxTree;
            var newRoot = oldTree.GetRoot().ReplaceNode(oldNode, newNode);
            var newTree = oldTree.WithRootAndOptions(newRoot, CSharpParseOptions.Default);
            compilation = compilation.ReplaceSyntaxTree(oldTree, newTree);
        }

        /// <summary>
        /// Finds the semantic symbol associated with an identifier with the specified name in the compilation.
        /// </summary>
        /// <param name="name">The string name of the identifier.</param>
        /// <returns>The semantic symbol. Throws an exception if no identifier is found.</returns>
        protected ISymbol FindSymbol(string name)
        {
            var s = compilation.GetTypeByMetadataName("FASTER.core." + name);
            if (s != null) return s;
            throw new NullReferenceException("symbol!");
        }

        public struct Template
        {
            public string TemplateName { get; }
            public string TemplateContents { get; }
            public Template(string name)
            {
                this.TemplateName = name;
                // "dotnet build" cannot handling resx's file resource, so using EmbeddedResource instead
                var asm = typeof(FASTER.core.IFasterKV<,,,,>).Assembly;
                foreach (var x in asm.GetManifestResourceNames())
                {
                    // resource name format is prefixed by root namespace and relative path separated by '.', like "FASTER.core.Index.FASTER.FASTER.cs"
                    if(x.EndsWith($".{name}.cs"))
                    {
                        using (var stm = asm.GetManifestResourceStream(x))
                        using (var sr = new StreamReader(stm, Encoding.UTF8))
                        {
                            this.TemplateContents = sr.ReadToEnd();
                            return;
                        }
                    }
                }
                throw new Exception($"cannot find resource:{name}");
            }
        }

    }
}
