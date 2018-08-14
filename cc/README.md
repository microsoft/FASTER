Building C++ FASTER
===================
The C++ version of FASTER uses CMake for builds. To build C++ FASTER, create
one or more build directories and use CMake to set up build scripts for your
target OS. Once CMake has generated the build scripts, it will try to update
them, as needed, during ordinary build.

Building on Windows
-------------------
Create new directory "build" off the root directory (FAST\cc). From the new
"build" directory, execute:

  cmake .. -G "<MSVC compiler> Win64"

To see a list of supported MSVC compiler versions, just run "cmake -G". As of
this writing, we're using Visual Studio 2017, so you would execute:

  cmake .. -G "Visual Studio 15 2017 Win64"

That will create build scripts inside your new "build" directory, including
a "FASTER.sln" file that you can use inside Visual Studio. CMake will add several
build profiles to FASTER.sln, including Debug/x64 and Release/x64.

Building on Linux
-----------------
The Linux build requires several packages (both libraries and header files);
see "CMakeFiles.txt" in the root directory (FAST/cc) for the list of libraries
being linked to, on Linux.

As of this writing, the required libraries are:
 - stdc++fs : for <experimental/filesytem>, used for cross-platform directory
              creation.
 - uuid : support for GUIDs.
 - tbb : Intel's Thread Building Blocks library, used for concurrent_queue.
 - gcc 
 - aio : Kernel Async I/O, used by QueueFile / QueueIoHandler.
 - stdc++
 - pthread : thread library.

Also, CMake on Linux, for the gcc compiler, generates build scripts for either
Debug or Release build, but not both; so you'll have to run CMake twice, in two
different directories, to get both Debug and Release build scripts.

Create new directories "build/Debug" and "build/Release" off the root directory
(FAST/cc). From "build/Debug", run:

  cmake -DCMAKE_BUILD_TYPE=Debug ../..

--and from "build/Release", run:

  cmake -DCMAKE_BUILD_TYPE=Release ../..

Then you can build Debug or Release binaries by running "make" inside the
relevant build directory.

Other options
-------------
You can try other generators (compilers) supported by CMake. The main CMake
build script is the CMakeLists.txt located in the root directory (FAST/cc).

Examples
========
There are some unit tests in FAST/cc/test.

Sum-store, located in FAST/cc/playground/sum_store-dir, is a good example of
checkpointing and recovery.

There's a basic YCSB test driver in FAST/cc/benchmark-dir.
