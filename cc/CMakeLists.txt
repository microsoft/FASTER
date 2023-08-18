cmake_minimum_required (VERSION 3.2.2)

set(CMAKE_CXX_STANDARD 14)

enable_testing()

include(ExternalProject)
project(FASTER)

# The set of additional options that we can pass into cmake. We currently support
# a flag `USE_BLOBS` that will link in azure's blob store library so that FASTER
# can be used with a blob device for the hybrid log.
OPTION(USE_BLOBS "Extend FASTER's hybrid log to blob store" OFF)
OPTION(USE_URING "Enable io_uring based IO handler" OFF)

if (MSVC)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /Zi /nologo /Gm- /W3 /WX /EHsc /GS /fp:precise /permissive- /Zc:wchar_t /Zc:forScope /Zc:inline /Gd /TP")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /FC /wd4996")

    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} /Od /RTC1 /MDd")
    set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} /O2 /Oi /Gy- /MD")

    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} /DEBUG /OPT:NOICF")
    set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} /DEBUG /OPT:NOICF")
else()
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++14")

    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -O0 -g -D_DEBUG")
    set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3 -g")

    if (USE_URING)
      set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -DFASTER_URING")
      set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -DFASTER_URING")
    endif()
endif()

#Always set _DEBUG compiler directive when compiling bits regardless of target OS
set_directory_properties(PROPERTIES COMPILE_DEFINITIONS_DEBUG "_DEBUG")

##### BEGIN GOOGLE TEST INSTALLATION #####
# Copied from https://github.com/google/googletest/tree/master/googletest#incorporating-into-an-existing-cmake-project
# Download and unpack googletest at configure time
configure_file(CMakeLists.txt.in googletest-download/CMakeLists.txt)
execute_process(COMMAND ${CMAKE_COMMAND} -G "${CMAKE_GENERATOR}" .
  RESULT_VARIABLE result
  WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/googletest-download)
if(result)
  message(FATAL_ERROR "CMake step for googletest failed: ${result}")
endif()
execute_process(COMMAND ${CMAKE_COMMAND} --build .
  RESULT_VARIABLE result
  WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/googletest-download )
if(result)
  message(FATAL_ERROR "Build step for googletest failed: ${result}")
endif()

# Prevent overriding the parent project's compiler/linker
# settings on Windows
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)

# Add googletest directly to our build. This defines
# the gtest and gtest_main targets.
add_subdirectory(${CMAKE_BINARY_DIR}/googletest-src
                 ${CMAKE_BINARY_DIR}/googletest-build
                 EXCLUDE_FROM_ALL)

##### END GOOGLE TEST INSTALLATION #####

include_directories(${CMAKE_SOURCE_DIR}/src)

# Set the directory targets when build in libs and binaries
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})

set (FASTER_LINK_LIBS
  faster
)

# Set the link libraries to for test compilation
set (FASTER_TEST_LINK_LIBS ${FASTER_LINK_LIBS} gtest)
if(WIN32)
  set(FASTER_TEST_LINK_LIBS ${FASTER_TEST_LINK_LIBS} rpcrt4)
else()
  set (FASTER_TEST_LINK_LIBS ${FASTER_TEST_LINK_LIBS} stdc++fs uuid tbb gcc aio m stdc++ pthread)
  if(USE_URING)
    set (FASTER_TEST_LINK_LIBS ${FASTER_TEST_LINK_LIBS} uring)
  endif()
  # Using blob storage. Link in appropriate libraries.
  if(USE_BLOBS)
    set (FASTER_TEST_LINK_LIBS ${FASTER_TEST_LINK_LIBS} azurestorage cpprest boost_system crypto ssl)
  endif()
endif()

# Set the link libraries to for benchmark binary compilation
set (FASTER_BENCHMARK_LINK_LIBS ${FASTER_LINK_LIBS})
if(WIN32)
set (FASTER_BENCHMARK_LINK_LIBS ${FASTER_LINK_LIBS} rpcrt4 wsock32 Ws2_32)
else()
  set (FASTER_BENCHMARK_LINK_LIBS ${FASTER_BENCHMARK_LINK_LIBS} stdc++fs uuid tbb gcc aio m stdc++ pthread)
  if(USE_URING)
    set (FASTER_BENCHMARK_LINK_LIBS ${FASTER_BENCHMARK_LINK_LIBS} uring)
  endif()
endif()

#Function to automate building test binaries
FUNCTION(ADD_FASTER_TEST TEST_NAME HEADERS)
  add_executable(${TEST_NAME} ${HEADERS} ${TEST_NAME}.cc)

  target_link_libraries(${TEST_NAME} ${FASTER_TEST_LINK_LIBS})
  add_test(${TEST_NAME} ${CMAKE_BINARY_DIR}/${TEST_NAME})
ENDFUNCTION()

#Function to automate building benchmark binaries
FUNCTION(ADD_FASTER_BENCHMARK BENCHMARK_NAME)
  add_executable(${BENCHMARK_NAME} ${BENCHMARK_HEADERS} ${BENCHMARK_NAME}.cc)

  target_link_libraries(${BENCHMARK_NAME} ${FASTER_BENCHMARK_LINK_LIBS})
ENDFUNCTION()

# Build each subdirectory
add_subdirectory(benchmark-dir)
add_subdirectory(playground)
add_subdirectory(src)
add_subdirectory(test)

# Compile tests for blob device too
if(USE_BLOBS)
  add_subdirectory(test/blobs)
endif()
