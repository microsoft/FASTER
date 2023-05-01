set(BENCHMARK_HEADERS
  file.h
)

set(BENCHMARK_SOURCES
)

# Legacy benchmark code
ADD_FASTER_BENCHMARK(benchmark)
add_executable(process_ycsb process_ycsb.cc)

# FASTER-LMHC benchmark code
ADD_FASTER_BENCHMARK(ycsb_benchmark)
target_link_libraries(ycsb_benchmark config++)

add_executable(ycsb_process ycsb_process.cc)
