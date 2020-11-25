#include <cstdio>
#include <fstream>
#include <iostream>
#include <stdlib.h>
#include <string>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

int main(int argc, char* argv[]) {
  if (argc != 3) {
    fprintf(stderr, "Requires two arguments: file copied from, file copied to.\n");
    exit(-1);
  }

  std::string from_filename{ argv[1] };
  std::string to_filename{ argv[2] };

  std::ifstream from_file{ from_filename };
  std::ofstream to_file{ to_filename };

  const std::string prefix{ "usertable user" };
  
  while (!from_file.eof()) {
    char buffer[256];
    from_file.clear();
    from_file.getline(buffer, sizeof(buffer));
    std::string line{ buffer };
    std::string::size_type pos = line.find(prefix);
    if (pos == std::string::npos) {
      continue;
    }
    line = line.substr(pos + prefix.size());
    uint64_t key = stol(line);

    to_file.write(reinterpret_cast<char*>(&key), sizeof(key));
  }
}
