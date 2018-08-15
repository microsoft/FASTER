set(SUM_STORE_HEADERS
  concurrent_recovery_test.h
  single_threaded_recovery_test.h
  sum_store.h
)

add_executable(sum_store ${SUM_STORE_HEADERS} sum_store.cc)
target_link_libraries(sum_store ${FASTER_BENCHMARK_LINK_LIBS})

