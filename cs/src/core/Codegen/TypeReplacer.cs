// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace FASTER.core.Roslyn
{
    class TypeReplacer : CSharpSyntaxRewriter
    {
        private readonly CSharpCompilation compilation;
        IDictionary<ISymbol, SyntaxNode> d;

        public TypeReplacer(CSharpCompilation compilation, IDictionary<ISymbol, SyntaxNode> d)
        {
            this.compilation = compilation;
            this.d = d;
        }

        public override SyntaxNode VisitIdentifierName(IdentifierNameSyntax node)
        {
            var s = compilation.GetTypeByMetadataName("FASTER.core." + node.Identifier.ValueText);
            if (s == null)
                return node;
            SyntaxNode replacementNode;
            if (d.TryGetValue(s, out replacementNode))
            {
                return replacementNode;
            }
            return node;
        }

    }

    class NamespaceReplacer : CSharpSyntaxRewriter
    {
        private readonly CSharpCompilation compilation;

        public NamespaceReplacer(CSharpCompilation compilation)
        {
            this.compilation = compilation;
        }

        public override SyntaxNode VisitNamespaceDeclaration(NamespaceDeclarationSyntax node)
        {
            //return base.VisitNamespaceDeclaration(node);
            var n = (SimpleNameSyntax)SyntaxFactory.ParseName("Codegen_" + this.compilation.AssemblyName);
            var namespaceName = SyntaxFactory.QualifiedName(node.Name, n);
            return node.WithName(namespaceName);

        }

    }

    class MultiDictionaryTypeReplacer : CSharpSyntaxRewriter
    {
        private readonly CSharpCompilation compilation;
        IDictionary<string, IDictionary<ISymbol, SyntaxNode>> dictionaryMap;

        public MultiDictionaryTypeReplacer(CSharpCompilation compilation, IDictionary<string, IDictionary<ISymbol, SyntaxNode>> dictionaries)
        {
            this.compilation = compilation;
            this.dictionaryMap = dictionaries;
        }

        public override SyntaxNode VisitClassDeclaration(ClassDeclarationSyntax node)
        {
            return RewriteTypeDefinition(node);
        }
        public override SyntaxNode VisitStructDeclaration(StructDeclarationSyntax node)
        {
            return RewriteTypeDefinition(node);
        }

        private SyntaxNode RewriteTypeDefinition(TypeDeclarationSyntax node)
        {
            var attrs = node.AttributeLists;
            var dictionaryMapKey =
                attrs
                .SelectMany(al =>
                al
                .Attributes
                .Where(a => a.Name.ToFullString().EndsWith("TypeKind"))
                .Select(a => a.ArgumentList.Arguments[0].ToFullString().Trim('"')))
                .FirstOrDefault();
            IDictionary<ISymbol, SyntaxNode> d;
            if (dictionaryMapKey != null && this.dictionaryMap.TryGetValue(dictionaryMapKey, out d))
            {
                var replacer = new TypeReplacer(this.compilation, d);
                return replacer.Visit(node);
            }
            else
            {
                return node;
            }
        }

    }

    public class TypeKindAttribute : Attribute
    {
        public TypeKindAttribute(string kind) { }
    }
}
