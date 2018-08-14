// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace FASTER.core.Roslyn
{
    class TypeReplacerCompiler : CompilerBase
    {
        protected readonly Type[] runtimeTypeParameters;
        protected readonly internal TypeMapper typeMapper;

        protected TypeReplacerCompiler(IEnumerable<string> templateNames, params Type[] types)
            : base(templateNames)
        {
            this.runtimeTypeParameters = new Type[types.Length];
            for (int i = 0; i < types.Length; i++)
            {
                var t = types[i];
                runtimeTypeParameters[i] = t;
            }
            typeMapper = new TypeMapper(runtimeTypeParameters);
        }

        protected void UpdateDictionary(Dictionary<ISymbol, SyntaxNode> d, ISymbol symbol, Type typeToUse)
        {
            var name = this.typeMapper.CSharpNameFor(typeToUse);
            var nodeToUse = SyntaxFactory.ParseTypeName(name);
            d.Add(symbol, nodeToUse);
        }
    }
}
