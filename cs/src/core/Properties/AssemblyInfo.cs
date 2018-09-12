// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

// General Information about an assembly is controlled through the following 
// set of attributes. Change these attribute values to modify the information
// associated with an assembly.
[assembly: AssemblyDescription("")]
[assembly: AssemblyCopyright("Copyright ©  2018")]
[assembly: AssemblyTrademark("")]
[assembly: AssemblyCulture("")]

// Setting ComVisible to false makes the types in this assembly not visible 
// to COM components.  If you need to access a type in this assembly from 
// COM, set the ComVisible attribute to true on that type.
[assembly: ComVisible(false)]

// The following GUID is for the ID of the typelib if this project is exposed to COM
[assembly: Guid("01002755-60ca-40ee-94d9-11c07eb58786")]

[assembly: InternalsVisibleTo("FASTER.test" + AssemblyRef.FASTERPublicKey)]

/// <summary>
/// Sets public key string for friend assemblies.
/// </summary>
static class AssemblyRef
{
    internal const string FASTERPublicKey = ", PublicKey=" +
        "0024000004800000940000000602000000240000525341310004000001000100fd9a530d3c8c7e" +
        "b70b5b7d31f98a7fd13196bf9b5dcfed4f0b96e5153cbb472d240b32233e7ff9af7bb93387e13e" +
        "b1f6560034e793ade406c979b061203ace4827ebb998954c41d27b67d920943cc7023af22261a2" +
        "140e41eeba38fdbc0579d3121605db53fc75512976026dc518e85a5ecb76fcc319fd56b1291782" +
        "9d137ec7";
}
