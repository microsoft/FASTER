cmake_minimum_required (VERSION 3.14)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

enable_testing()

include(FetchContent)
project(FASTER)

# The set of additional options that we can pass into cmake. We currently support
# a flag `USE_BLOBS` that will link in azure's blob store library so that FASTER
# can be used with a blob device for the hybrid log.
OPTION(USE_BLOBS "Extend FASTER's hybrid log to blob store" OFF)

if (MSVC)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /Zi /nologo /Gm- /W3 /WX /EHsc /GS /fp:precise /permissive- /Zc:wchar_t /Zc:forScope /Zc:inline /Gd /TP")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /FC /wd4996")

    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} /Od /RTC1 /MDd")
    set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} /O2 /Oi /Gy- /MD")

    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} /DEBUG /OPT:REF /OPT:NOICF /INCREMENTAL:NO")
    set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} /DEBUG /OPT:REF /OPT:NOICF /INCREMENTAL:NO")
else()
    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -Og -g3 -D_DEBUG -DTOML_CONFIG -DSTATISTICS")
    set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O2 -g -DTOML_CONFIG")
endif()

#Always set _DEBUG compiler directive when compiling bits regardless of target OS
set_directory_properties(PROPERTIES COMPILE_DEFINITIONS_DEBUG "_DEBUG")

##### BEGIN GOOGLE TEST INSTALLATION #####
FetchContent_Declare(
  googletest
  GIT_REPOSITORY https://github.com/google/googletest.git
  GIT_TAG v1.14.0
)
set(BUILD_GMOCK OFF)
# For Windows: Prevent overriding the parent project's compiler/linker settings
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)
##### END GOOGLE TEST INSTALLATION #####

#### BEGIN TOML11 INSTALLATION #####
FetchContent_Declare(
  toml11
  GIT_REPOSITORY https://github.com/ToruNiina/toml11
  GIT_TAG v3.8.1
)
FetchContent_MakeAvailable(toml11)
include_directories(${toml11_SOURCE_DIR})
#### END TOML11 INSTALLATION #####

include_directories(${CMAKE_SOURCE_DIR}/src)

# Set the directory targets when build in libs and binaries
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})

set (FASTER_LINK_LIBS
  faster
)

# Set the link libraries to for test compilation
set (FASTER_TEST_LINK_LIBS ${FASTER_LINK_LIBS} gtest_main)
if(WIN32)
  set(FASTER_TEST_LINK_LIBS ${FASTER_TEST_LINK_LIBS} rpcrt4)
else()
  set (FASTER_TEST_LINK_LIBS ${FASTER_TEST_LINK_LIBS} stdc++fs uuid tbb gcc aio m stdc++ pthread)
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
endif()

#Function to automate building test binaries
FUNCTION(ADD_FASTER_TEST TEST_NAME HEADERS)
  add_executable(${TEST_NAME} ${HEADERS} ${TEST_NAME}.cc)

  target_link_libraries(${TEST_NAME} ${FASTER_TEST_LINK_LIBS})
  target_include_directories(${TEST_NAME} PRIVATE ${toml11_SOURCE_DIR})
  add_test(${TEST_NAME} ${CMAKE_BINARY_DIR}/${TEST_NAME})
ENDFUNCTION()

#Function to automate building benchmark binaries
FUNCTION(ADD_FASTER_BENCHMARK BENCHMARK_NAME)
  add_executable(${BENCHMARK_NAME} ${BENCHMARK_HEADERS} ${BENCHMARK_NAME}.cc)

  target_link_libraries(${BENCHMARK_NAME} ${FASTER_BENCHMARK_LINK_LIBS})
  target_include_directories(${BENCHMARK_NAME} PRIVATE ${toml11_SOURCE_DIR})
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
