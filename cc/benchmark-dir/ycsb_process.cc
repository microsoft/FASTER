#include <cstdio>
#include <fstream>
#include <iostream>
#include <stdlib.h>
#include <string>
#include <vector>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

enum class Op : uint64_t {
  Invalid = 0,

  Read = 1,
  Upsert = 2,
  RMW = 3,
  Insert = 4,
  Scan = 5,
  Delete = 6,
};
static_assert(sizeof(Op) == 8);

/*
void process_load_file(std::ifstream& from_file, std::ofstream& to_file, uint32_t value_sz) {
  printf("Processing == LOAD == file\n");

  const std::string prefix{ "INSERT usertable user" };
  const std::string value_needle { "[ field0=" };

  std::string buffer;
  buffer.reserve(64 * 1024);

  uint64_t key;
  std::string::size_type value_pos;

  uint32_t n_lines = 0;
  while (!from_file.eof()) {
    ++n_lines;
    std::getline(from_file, buffer);

    // check if valid line
    if (buffer.compare(0, prefix.size(), prefix) != 0) {
      continue;
    }
    // extract key
    key = std::stol(buffer.data() + prefix.size());
    // extract value
    value_pos = buffer.find(value_needle);
    if (value_pos == std::string::npos) {
      throw std::runtime_error{ "Could not locate value "};
    }

    // write key & value
    to_file.write(reinterpret_cast<char*>(&key), sizeof(key));
    to_file.write(buffer.data() + value_pos + value_needle.size(), value_sz);
  }
  to_file.flush();

  from_file.close();
  to_file.close();

  printf("\tRead %u lines\n", n_lines);
}

void process_run_file(std::ifstream& from_file, std::ofstream& to_file, uint32_t value_sz) {
  printf("Processing == RUN == file\n");

  const std::string read_prefix{ "READ usertable user" };
  const std::string update_prefix{ "UPDATE usertable user" };
  const std::string rmw_prefix{ "RMW usertable user" };
  const std::string insert_prefix{ "INSERT usertable user" };

  std::vector<std::pair<std::string, Op>> prefixes{
    std::make_pair(update_prefix, Op::Upsert),
    std::make_pair(read_prefix, Op::Read),
    std::make_pair(rmw_prefix, Op::RMW),
    std::make_pair(insert_prefix, Op::Insert) };
  const std::string value_needle { "[ field0=" };

  std::string buffer;
  buffer.reserve(64 * 1024);

  Op operation;
  uint64_t key;
  std::string key_prefix;
  std::string::size_type value_pos;

  uint32_t n_lines = 0;
  while (!from_file.eof()) {
    ++n_lines;
    std::getline(from_file, buffer);

    // check if valid line
    operation = Op::Invalid;
    for (auto& needle : prefixes) {
      if (buffer.compare(0, needle.first.size(), needle.first) != 0) {
        continue;
      }
      key_prefix = needle.first;
      operation = needle.second;
      break;
    }
    if (operation == Op::Invalid || operation == Op::Scan) {
      continue;
    }

    // extract key
    key = std::stol(buffer.data() + key_prefix.size());

    // write op & key
    to_file.write(reinterpret_cast<char*>(&operation), sizeof(operation));
    to_file.write(reinterpret_cast<char*>(&key), sizeof(key));

    if (operation == Op::Read) {
      // no value field
      continue;
    }

    // extract value
    value_pos = buffer.find(value_needle);
    if (value_pos == std::string::npos) {
      throw std::runtime_error{ "Could not locate value "};
    }
    // write value
    to_file.write(buffer.data() + value_pos + value_needle.size(), value_sz);
  }
  to_file.flush();

  from_file.close();
  to_file.close();

  printf("\tRead %u lines\n", n_lines);
}
*/

bool rmw_updates = false;

void process_load_file(std::ifstream& from_file, std::ofstream& to_file) {
  printf("Processing == LOAD == file\n");

  const std::string prefix{ "INSERT usertable user" };

  std::string buffer;
  buffer.reserve(64 * 1024);

  uint64_t key;
  uint32_t n_lines = 0;
  while (!from_file.eof()) {
    ++n_lines;
    std::getline(from_file, buffer);

    // check if valid line
    if (buffer.compare(0, prefix.size(), prefix) != 0) {
      continue;
    }
    // extract key
    key = std::stol(buffer.data() + prefix.size());
    // write key & value
    to_file.write(reinterpret_cast<char*>(&key), sizeof(key));
  }
  to_file.flush();

  from_file.close();
  to_file.close();

  printf("\tRead %u lines\n", n_lines);
}

void process_run_file(std::ifstream& from_file, std::ofstream& to_file) {
  printf("Processing == RUN == file\n");

  const std::string read_prefix{ "READ usertable user" };
  const std::string update_prefix{ "UPDATE usertable user" };
  const std::string insert_prefix{ "INSERT usertable user" };

  std::vector<std::pair<std::string, Op>> prefixes{
    std::make_pair(update_prefix, !rmw_updates ? Op::Upsert : Op::RMW),
    std::make_pair(read_prefix, Op::Read),
    std::make_pair(insert_prefix, Op::Insert) };

  std::string buffer;
  buffer.reserve(64 * 1024);

  Op operation;
  uint64_t key;
  std::string key_prefix;

  uint32_t n_lines = 0;
  while (!from_file.eof()) {
    ++n_lines;
    std::getline(from_file, buffer);

    // check if valid line
    operation = Op::Invalid;
    for (auto& needle : prefixes) {
      if (buffer.compare(0, needle.first.size(), needle.first) != 0) {
        continue;
      }
      key_prefix = needle.first;
      operation = needle.second;
      break;
    }
    if (operation == Op::Invalid) {
      continue;
    }

    // extract key
    key = std::stol(buffer.data() + key_prefix.size());

    // write op & key
    to_file.write(reinterpret_cast<char*>(&operation), sizeof(operation));
    to_file.write(reinterpret_cast<char*>(&key), sizeof(key));
  }
  to_file.flush();

  from_file.close();
  to_file.close();

  printf("\tRead %u lines\n", n_lines);
}


int main(int argc, char* argv[]) {
  if (argc != 4 && argc != 5) {
    fprintf(stderr, "./process_ycsb <file_type> <file copied from> <file copied to> [rmw_updates={0,1}]\n");
    return 1;
  }

  std::string file_type{ argv[1] };
  if (file_type != "load" && file_type != "run") {
    fprintf(stderr, "Invalid file type: 'load' and 'run' are valid types\n");
    return 1;
  }
  //uint32_t value_sz = std::stol(argv[2]);
  std::string from_filename{ argv[2] };
  std::string to_filename{ argv[3] };

  if (argc == 5) {
    rmw_updates = std::stol(argv[4]) != 0;
  }
  fprintf(stderr, "!! WARNING !!: YCSB's 'UPDATE' are treated like '%s'\n\n",
          rmw_updates ? "RMWs" : "UPDATEs");

  // Open & check files
  std::ifstream from_file{ from_filename };
  std::ofstream to_file{ to_filename };
  if (from_file.fail()) {
    throw std::runtime_error{ "Failed to open file(s) "};
  }

  constexpr uint32_t BUFFER_SIZE = 1024 * 1024;
  char from_buffer[BUFFER_SIZE];
  from_file.rdbuf()->pubsetbuf(from_buffer, BUFFER_SIZE);


  // Process file
  if (file_type == "load") {
    process_load_file(from_file, to_file);
  } else if (file_type == "run") {
    process_run_file(from_file, to_file);
  } else {
    throw std::invalid_argument{ "invalid file type "};
  }

  return 0;
}
