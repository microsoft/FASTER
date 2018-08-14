set(BENCHMARK_HEADERS
  file.h
)

set(BENCHMARK_SOURCES
)

ADD_FAST_BENCHMARK(benchmark)

add_executable(process_ycsb process_ycsb.cc)
