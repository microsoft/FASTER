set(BENCHMARK_HEADERS
  file.h
)

set(BENCHMARK_SOURCES
)

# Legacy benchmark code
ADD_FASTER_BENCHMARK(benchmark)
add_executable(process_ycsb process_ycsb.cc)


add_executable(ycsb_process ycsb_process.cc)
