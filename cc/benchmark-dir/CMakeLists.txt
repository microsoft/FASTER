set(BENCHMARK_HEADERS
  benchmark.h
  file.h
)

set(BENCHMARK_SOURCES
)

# Legacy benchmark code
ADD_FASTER_BENCHMARK(benchmark)
ADD_FASTER_BENCHMARK(benchmark_f2)
add_executable(process_ycsb process_ycsb.cc)
