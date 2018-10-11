// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using static FASTER.core.Roslyn.Helper;

namespace FASTER.core.Roslyn
{
    class MixedBlitManagedFasterHashTableCompiler<TKey, TValue, TInput, TOutput, TContext, TFunctions> : TypeReplacerCompiler
    {
        private readonly bool treatValueAsAtomic;

        private MixedBlitManagedFasterHashTableCompiler(bool treatValueAsAtomic)
        : base(SourceNames(treatValueAsAtomic),
              typeof(TKey),
              typeof(TValue),
              typeof(TInput),
              typeof(TOutput),
              typeof(TContext),
              typeof(TFunctions)
              )
        {
            this.treatValueAsAtomic = treatValueAsAtomic;
        }
        private static IEnumerable<string> SourceNames(bool treatValueAsAtomic)
        {
                return new string[] {
                    "MixedKeyWrapper",
                    "MixedValueWrapper",
                    "MixedInputWrapper",
                    "MixedOutputWrapper",
                    "MixedContextWrapper",
                    "MixedFunctionsWrapper",
                    "IFASTER_Mixed",
                    "MixedManagedFAST",
            };
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns>The generated type (to be instantiated). If null, then the error messages giving the reason for failing to generate the type.</returns>
        public static Tuple<Type, string> GenerateGenericFasterHashTableClass(long size, IDevice logDevice, bool treatValueAsAtomic, bool persistGeneratedCode, bool optimizeCode)
        {
            var c = new MixedBlitManagedFasterHashTableCompiler<TKey, TValue, TInput, TOutput, TContext, TFunctions>(treatValueAsAtomic);
            c.Run(persistGeneratedCode, optimizeCode);
            var name = String.Format("FASTER.core.Codegen_{0}", c.compilation.AssemblyName);
            var t = c.Compile(persistGeneratedCode);
            var a = t.Item1;

            if (a == null)
            {
                string error = "Errors during code-gen compilation: \n" + t.Item2;
                throw new Exception(error);
            }

            var managedFastClassType = a.GetType(name + ".MixedManagedFast");
            return Tuple.Create(managedFastClassType, t.Item2);
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

            var userKeyTypeName = this.typeMapper.CSharpNameFor(typeof(TKey));
            var userValueTypeName = this.typeMapper.CSharpNameFor(typeof(TValue));
            var userInputTypeName = this.typeMapper.CSharpNameFor(typeof(TInput));
            var userOutputTypeName = this.typeMapper.CSharpNameFor(typeof(TOutput));
            var userContextTypeName = this.typeMapper.CSharpNameFor(typeof(TContext));

            var internalKeyTypeName = userKeyTypeName;
            var internalValueTypeName = userValueTypeName;
            var internalInputTypeName = userInputTypeName;
            var internalOutputTypeName = userOutputTypeName;
            var internalContextTypeName = userContextTypeName;

            #region Things needed before creating the compilation

            var preprocessorSymbols = new List<string>();
            string internalWrappedTypes = "";

            if (IsBlittable<TKey>())
            {
                var tKeyType = typeof(TKey);
                preprocessorSymbols.Add("BLIT_KEY");
                if (tKeyType.IsGenericType)
                {
                    preprocessorSymbols.Add("GENERIC_BLIT_KEY");
                    internalKeyTypeName = "Key_" + String.Join("_", tKeyType.GenericTypeArguments.Select(t => t.GetCSharpSourceSyntax().CleanUpIdentifierName()));
                    internalWrappedTypes += GenerateInternalWrappedType<TKey>(internalKeyTypeName);
                }
            }

            if (this.treatValueAsAtomic)
            {
                preprocessorSymbols.Add("VALUE_ATOMIC");
            }
            if (IsBlittable<TValue>())
            {
                var tValueType = typeof(TValue);
                preprocessorSymbols.Add("BLIT_VALUE");
                if (tValueType.IsGenericType)
                {
                    preprocessorSymbols.Add("GENERIC_BLIT_VALUE");
                    internalValueTypeName = "Value_" + String.Join("_", tValueType.GenericTypeArguments.Select(t => t.GetCSharpSourceSyntax().CleanUpIdentifierName()));
                    internalWrappedTypes += GenerateInternalWrappedType<TValue>(internalValueTypeName);
                }
            }
            if (IsBlittable<TInput>())
            {
                var tInputType = typeof(TInput);
                preprocessorSymbols.Add("BLIT_INPUT");
                if (tInputType.IsGenericType)
                {
                    preprocessorSymbols.Add("GENERIC_BLIT_INPUT");
                    internalInputTypeName = "Input_" + String.Join("_", tInputType.GenericTypeArguments.Select(t => t.GetCSharpSourceSyntax().CleanUpIdentifierName()));
                    internalWrappedTypes += GenerateInternalWrappedType<TInput>(internalInputTypeName);
                }
            }

            if (IsBlittable<TOutput>())
            {
                var tOutputType = typeof(TOutput);
                preprocessorSymbols.Add("BLIT_OUTPUT");
                if (tOutputType.IsGenericType)
                {
                    preprocessorSymbols.Add("GENERIC_BLIT_OUTPUT");
                    internalOutputTypeName = "Output_" + String.Join("_", tOutputType.GenericTypeArguments.Select(t => t.GetCSharpSourceSyntax().CleanUpIdentifierName()));
                    internalWrappedTypes += GenerateInternalWrappedType<TOutput>(internalOutputTypeName);
                }
            }

            if (IsBlittable<TContext>())
            {
                var tContextType = typeof(TContext);
                preprocessorSymbols.Add("BLIT_CONTEXT");
                if (tContextType.IsGenericType)
                {
                    preprocessorSymbols.Add("GENERIC_BLIT_CONTEXT");
                    internalContextTypeName = "Context_" + String.Join("_", tContextType.GenericTypeArguments.Select(t => t.GetCSharpSourceSyntax().CleanUpIdentifierName()));
                    internalWrappedTypes += GenerateInternalWrappedType<TContext>(internalContextTypeName);
                }
            }

            #endregion

            // side-effect: creates this.compilation
            CreateCompilation(persistGeneratedCode, optimizeCode, preprocessorSymbols: preprocessorSymbols);

            foreach (var rtTP in this.runtimeTypeParameters)
            {
                AddAssemblyReferencesNeededFor(rtTP);
            }

            var d = new Dictionary<string, IDictionary<ISymbol, SyntaxNode>>();

            var userDictionary = new Dictionary<ISymbol, SyntaxNode>
            {
                { FindSymbol("MixedKey"), SyntaxFactory.ParseTypeName(userKeyTypeName) },
                { FindSymbol("MixedValue"), SyntaxFactory.ParseTypeName(userValueTypeName) },
                { FindSymbol("MixedInput"), SyntaxFactory.ParseTypeName(userInputTypeName) },
                { FindSymbol("MixedOutput"), SyntaxFactory.ParseTypeName(userOutputTypeName) },
                { FindSymbol("MixedContext"), SyntaxFactory.ParseTypeName(userContextTypeName) },
                { FindSymbol("MixedUserFunctions"), SyntaxFactory.ParseTypeName(this.typeMapper.CSharpNameFor(typeof(TFunctions))) }
            };

            d.Add("user", userDictionary);

            var internalDictionary = new Dictionary<ISymbol, SyntaxNode>
            {
                { FindSymbol("MixedKey"), SyntaxFactory.ParseTypeName(internalKeyTypeName) },
                { FindSymbol("MixedValue"), SyntaxFactory.ParseTypeName(internalValueTypeName) },
                { FindSymbol("MixedInput"), SyntaxFactory.ParseTypeName(internalInputTypeName) },
                { FindSymbol("MixedOutput"), SyntaxFactory.ParseTypeName(internalOutputTypeName) },
                { FindSymbol("MixedContext"), SyntaxFactory.ParseTypeName(internalContextTypeName) },
                { FindSymbol("MixedUserFunctions"), SyntaxFactory.ParseTypeName(this.typeMapper.CSharpNameFor(typeof(TFunctions))) }
            };

            d.Add("internal", internalDictionary);

            var pass1 = new MultiDictionaryTypeReplacer(this.compilation, d);
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
                var compilationSyntax = (CompilationUnitSyntax)newTree.GetRoot();
                compilationSyntax = compilationSyntax.AddUsings(usingFASTDotCore);
                newTree = newTree
                    .WithRootAndOptions(compilationSyntax, CSharpParseOptions.Default);

                compilation = compilation.ReplaceSyntaxTree(oldTree, newTree);
            }

            #region Create new source files from scratch (instead of from a template): it does *not* get transformed

            if (!String.IsNullOrWhiteSpace(internalWrappedTypes))
            {
                internalWrappedTypes =
                    "using System;\r\n" +
                    "using System.Runtime.CompilerServices;\r\n" +
                    "using System.Runtime.InteropServices;\r\n" +
                    $"namespace FASTER.core.Codegen_{this.compilation.AssemblyName}\r\n" +
                    "{\r\n" +
                    internalWrappedTypes +
                    "}\r\n"
                    ;

                this.AddSource(internalWrappedTypes, "InternalWrappedTypes");
            }

            #endregion



#if TIMING
            sw.Stop();
            System.Diagnostics.Debug.WriteLine("Time to run the FasterHashTable compiler: {0}ms", sw.ElapsedMilliseconds);
            using (var fileStream = new StreamWriter("foo.txt", true))
            {
                fileStream.WriteLine("Time to run the FasterHashTable compiler: {0}ms", sw.ElapsedMilliseconds);
            }
#endif
        }

        private static string GenerateInternalWrappedType<T>(string internalTypeName)
        {
            return
                $"public unsafe struct {internalTypeName} {{\r\n" +
                $"   public fixed byte fixedBuffer[{Helper.GetSize(default(T))}];" +
                $"}}\r\n"
                ;
        }
    }
}
