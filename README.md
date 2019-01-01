ServiceMq
=============

Easy store and forward message queue library for .NET.

### Breaking Changes in version 5.1.0

1. Updated to NetCoreApp 2.0 and 2.2 and .NET Framework 4.62 

2. Updated to ServiceWire 5.1.0. 

3. Dropped support for .NET 3.5.

### Breaking Changes in version 4.0.0

1. Relies on ServiceWire 4.0.1 and Newtonsoft.Json 6.0.6 for communications and serialization. Removes ServiceStack.Text because it was unable to serialize structs.

2. Adds support for .NET 3.5 via the TaskParallelLibrary 1.0.2856.0 package from Microsoft on NuGet.

3. Adds strong name so that it can be used by other projects that require strong naming.

NOTE: This version will not operate with previous versions.