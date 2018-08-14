// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Emit;
using Microsoft.CodeAnalysis.Scripting.Hosting;
using System;
using System.Collections.Generic;
using System.Diagnostics.Contracts;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
#if DOTNETCORE
using System.Runtime.Loader;
#endif
using static FASTER.core.Roslyn.Helper;


namespace FASTER.core.Roslyn
{
    class RoslynHelpers
    {
        public static void DebugPPrint(SyntaxNode node)
        {
            System.Diagnostics.Debug.WriteLine(node.NormalizeWhitespace().ToFullString());
        }

        public class LocalsRenamer : CSharpSyntaxRewriter
        {
            readonly SemanticModel model;
            readonly Func<ISymbol, string> rename;

            public LocalsRenamer(SemanticModel model, Func<ISymbol, string> rename)
            {
                this.model = model;
                this.rename = rename;
            }

            public override SyntaxNode VisitVariableDeclarator(VariableDeclaratorSyntax node)
            {
                var symbol = model.GetDeclaredSymbol(node);
                if (symbol != null)
                {
                    string newName = rename(symbol);
                    if (newName != null)
                    {
                        return node.WithIdentifier(node.Identifier.CopyAnnotationsTo(SyntaxFactory.Identifier(newName)));
                    }
                }
                return base.VisitVariableDeclarator(node);
            }

            public override SyntaxNode VisitIdentifierName(IdentifierNameSyntax node)
            {
                var symbol = model.GetSymbolInfo(node).Symbol;
                if (symbol != null)
                {
                    string newName = rename(symbol);
                    if (newName != null)
                    {
                        return node.WithIdentifier(node.Identifier.CopyAnnotationsTo(SyntaxFactory.Identifier(newName)));
                    }
                }
                return base.VisitIdentifierName(node);
            }
        }

        public static CSharpCompilation ReplaceConstantValue(CSharpCompilation c, string constantName, string newValue)
        {
            foreach (var t in c.SyntaxTrees)
            {
                var oldTree = t;
                var root = t.GetRoot();

                var variableDeclarations = root.DescendantNodes()
                    .OfType<FieldDeclarationSyntax>()
                    .Where(fds => fds.Modifiers.Any(m => m.Kind() == SyntaxKind.ConstKeyword))
                    .SelectMany(fds => fds.Declaration.Variables.Where(v => v.Identifier.ValueText == constantName))
                    ;

                foreach (var variableDeclaration in variableDeclarations)
                {

                    var newNode = variableDeclaration.WithInitializer(
                        SyntaxFactory.EqualsValueClause(SyntaxFactory.ParseExpression(newValue)));
                    root = root.ReplaceNode(variableDeclaration, newNode);
                    var newTree = oldTree
                        .WithRootAndOptions(root, CSharpParseOptions.Default)
                        ;
                    c = c.ReplaceSyntaxTree(oldTree, newTree);
                }
            }
            return c;
        }

        public static Dictionary<Assembly, MetadataReference> metadataReferenceCache = new Dictionary<Assembly, MetadataReference>();
        private static InteractiveAssemblyLoader loader = new InteractiveAssemblyLoader();
        internal static Assembly EmitCompilationAndLoadAssembly(CSharpCompilation compilation, bool makeAssemblyDebuggable, out string errorMessages)
        {
            Assembly a = null;
            EmitResult emitResult;

            if (makeAssemblyDebuggable)
            {
                var assemblyFileName = compilation.SourceModule.Name;
                string directory = null;
                foreach (var tree in compilation.SyntaxTrees)
                {
                    if (directory == null)
                    {
                        directory = Path.GetDirectoryName(tree.FilePath);
                    }
                    var baseFile = tree.FilePath;
                    var sourceFile = Path.ChangeExtension(baseFile, ".cs");
                    File.WriteAllText(sourceFile, tree.GetRoot().ToFullString());
                }
                var assemblyPath = Path.Combine(directory, assemblyFileName);
                var debugSymbolsFile = Path.ChangeExtension(assemblyPath, ".pdb");
                var emitOptions = new EmitOptions().WithDebugInformationFormat(DebugInformationFormat.PortablePdb);
                using (var peStream = new FileStream(assemblyPath, FileMode.CreateNew))
                using (var pdbStream = new FileStream(debugSymbolsFile, FileMode.CreateNew))
                {
                    emitResult = compilation.Emit(peStream, pdbStream: pdbStream, options: emitOptions);
                    if (emitResult.Success)
                    {
                        peStream.Close();
                        pdbStream.Close();
#if DOTNETCORE
                        a = AssemblyLoadContext.Default.LoadFromAssemblyPath(Path.GetFullPath(assemblyPath));
#else
                        a = Assembly.LoadFrom(assemblyPath);
#endif
                    }
                }
            }
            else
            {
                using (var peStream = new MemoryStream())
                {
                    emitResult = compilation.Emit(peStream);
                    if (emitResult.Success)
                    {
                        peStream.Position = 0;
#if DOTNETCORE
                        a = AssemblyLoadContext.Default.LoadFromStream(peStream);
                        peStream.Position = 0; // Must reset it! Loading leaves its position at the end
#else
                        var assembly = peStream.ToArray();
                        a = Assembly.Load(assembly);
#endif
                        loader.RegisterDependency(a);
                        var aref = MetadataReference.CreateFromStream(peStream);
                        metadataReferenceCache.Add(a, aref);
                    }
                }
            }

            errorMessages = string.Join("\n", emitResult.Diagnostics);

            if (makeAssemblyDebuggable && emitResult.Diagnostics.Any(d => d.Severity == DiagnosticSeverity.Error))
                System.Diagnostics.Debug.WriteLine(errorMessages);

            return a;
        }

        internal static IEnumerable<MetadataReference> MetadataReferencesNeededForType(Type type)
        {
            HashSet<Type> closure = new HashSet<Type>();
            CollectAssemblyReferences(type, closure);
            var result = closure
                .Where(t => !t.GetTypeInfo().Assembly.IsDynamic)
                .Where(t => metadataReferenceCache.ContainsKey(t.GetTypeInfo().Assembly) || Path.IsPathRooted(t.GetTypeInfo().Assembly.Location))
                //.Select(t => String.IsNullOrWhiteSpace(t.GetTypeInfo().Assembly.Location) && metadataReferenceCache.ContainsKey(t.GetTypeInfo().Assembly) ? metadataReferenceCache[t.GetTypeInfo().Assembly] : MetadataReference.CreateFromFile(t.GetTypeInfo().Assembly.Location))
                .Select(t => metadataReferenceCache.ContainsKey(t.GetTypeInfo().Assembly) ? metadataReferenceCache[t.GetTypeInfo().Assembly] : MetadataReference.CreateFromFile(t.GetTypeInfo().Assembly.Location))
                .Distinct(metadataReferenceComparer)
                ;
            return result;
        }
        internal static void CollectAssemblyReferences(Type t, HashSet<Type> partialClosure)
        {
            if (partialClosure.Add(t))
            {
                if (t.GetTypeInfo().BaseType != null)
                    CollectAssemblyReferences(t.GetTypeInfo().BaseType, partialClosure);
                if (t.IsNested)
                    CollectAssemblyReferences(t.DeclaringType, partialClosure);
                foreach (var j in t.GetTypeInfo().GetInterfaces())
                {
                    CollectAssemblyReferences(j, partialClosure);
                }
                foreach (var genericArgument in t.GenericTypeArguments)
                {
                    CollectAssemblyReferences(genericArgument, partialClosure);
                }
                foreach (var f in t.GetTypeInfo().GetFields(BindingFlags.Public | BindingFlags.Instance))
                {
                    CollectAssemblyReferences(f.FieldType, partialClosure);
                }
                foreach (var p in t.GetTypeInfo().GetProperties(BindingFlags.Public | BindingFlags.Instance))
                {
                    CollectAssemblyReferences(p.PropertyType, partialClosure);
                }
            }
        }

        internal class MetadataReferenceComparer : IEqualityComparer<MetadataReference>
        {
            public bool Equals(MetadataReference x, MetadataReference y)
            {
                return x.Display.Equals(y.Display);
            }

            public int GetHashCode(MetadataReference obj)
            {
                return obj.Display.GetHashCode();
            }
        }
        internal static IEqualityComparer<MetadataReference> metadataReferenceComparer = new MetadataReferenceComparer();
    }
}
