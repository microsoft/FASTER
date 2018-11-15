// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using static Microsoft.CodeAnalysis.CSharp.SyntaxFactory;
using static FASTER.core.Roslyn.Helper;
using System.Globalization;

namespace FASTER.core.Roslyn
{
    class FasterHashTableCompiler<TKey, TValue, TInput, TOutput, TContext, TFunctions, TIFaster> : TypeReplacerCompiler
    {
        private FasterHashTableCompiler()
            : base(sourceNames, typeof(TKey), typeof(TValue), typeof(TInput), typeof(TOutput), typeof(TContext), typeof(TFunctions), typeof(TIFaster))
        {
        }

        private static IEnumerable<string> sourceNames = new string[] {
                "FASTER",
                "FASTERImpl",
                "FASTERThread",
                "AsyncIO",
                "Checkpoint",
                "Contexts",
                "Layout",
                "AsyncResultTypes",
                "FASTERBase",
                "Recovery",
                "IndexRecovery",
                "IndexCheckpoint",
                "StateTransitions",
        };


    /// <summary>
    /// 
    /// </summary>
    /// <returns>The generated type (to be instantiated). If null, then the error messages giving the reason for failing to generate the type.</returns>
    public static Tuple<Type, string> GenerateFasterHashTableClass(bool persistGeneratedCode, bool optimizeCode)
        {
            var c = new FasterHashTableCompiler<TKey, TValue, TInput, TOutput, TContext, TFunctions, TIFaster>();
            c.Run(persistGeneratedCode, optimizeCode);
            var name = String.Format("FASTER.core.Codegen_{0}.FasterKV", c.compilation.AssemblyName);
            var r = c.Compile(persistGeneratedCode);
            var a = r.Item1;

            if (a == null)
            {
                string error = "Errors during code-gen compilation: \n" + r.Item2;
                throw new Exception(error);
            }
            return Tuple.Create(a.GetType(name), r.Item2);
        }

        /// <summary>
        /// Runs the transformations needed to produce a valid compilation unit.
        /// </summary>
        public void Run(bool persistGeneratedCode, bool optimizeCode)
        {
#if TIMING
            Stopwatch sw = new Stopwatch();
            sw.Start();
#endif
            // side-effect: creates this.compilation
            CreateCompilation(persistGeneratedCode, optimizeCode);

            foreach (var rtTP in this.runtimeTypeParameters)
            {
                AddAssemblyReferencesNeededFor(rtTP);
            }

            var d = new Dictionary<ISymbol, SyntaxNode>();
            var i = 0;
            UpdateDictionary(d, FindSymbol("Key"), this.runtimeTypeParameters.ElementAt(i++));
            UpdateDictionary(d, FindSymbol("Value"), this.runtimeTypeParameters.ElementAt(i++));
            UpdateDictionary(d, FindSymbol("Input"), this.runtimeTypeParameters.ElementAt(i++));
            UpdateDictionary(d, FindSymbol("Output"), this.runtimeTypeParameters.ElementAt(i++));
            UpdateDictionary(d, FindSymbol("Context"), this.runtimeTypeParameters.ElementAt(i++));
            UpdateDictionary(d, FindSymbol("Functions"), this.runtimeTypeParameters.ElementAt(i++));
            UpdateDictionary(d, FindSymbol("IFasterKV"), this.runtimeTypeParameters.ElementAt(i++));

            var pass1 = new TypeReplacer(this.compilation, d);
            var pass2 = new NamespaceReplacer(this.compilation);

            var FASTDotCoreNamespaceName = SyntaxFactory.QualifiedName(SyntaxFactory.IdentifierName("FASTER"), SyntaxFactory.IdentifierName("core"));
            var usingFASTDotCore = SyntaxFactory.UsingDirective(FASTDotCoreNamespaceName);

            foreach (var t in compilation.SyntaxTrees)
            {
                var oldTree = t;
                var oldNode = t.GetRoot();
                var newNode = pass1.Visit(oldNode);
                newNode = pass2.Visit(newNode);

                var newRoot = oldTree.GetRoot().ReplaceNode(oldNode, newNode);
                var newTree = oldTree
                    .WithRootAndOptions(newRoot, CSharpParseOptions.Default)
                    ;
                var compilationSyntax = (CompilationUnitSyntax) newTree.GetRoot();
                compilationSyntax = compilationSyntax.AddUsings(usingFASTDotCore);
                newTree = newTree
                    .WithRootAndOptions(compilationSyntax, CSharpParseOptions.Default);

                compilation = compilation.ReplaceSyntaxTree(oldTree, newTree);
            }

#if TIMING
            sw.Stop();
            System.Diagnostics.Debug.WriteLine("Time to run the FasterHashTable compiler: {0}ms", sw.ElapsedMilliseconds);
            using (var fileStream = new StreamWriter("foo.txt", true))
            {
                fileStream.WriteLine("Time to run the FasterHashTable compiler: {0}ms", sw.ElapsedMilliseconds);
            }
#endif
        }

    }
}
