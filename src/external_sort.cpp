#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <stdexcept>

#include <external_sort.hpp>
#include <getopt.h>
#include <stdlib.h>

struct parsed_options {
  std::string input_file;
  std::string output_file;
  std::string tmp_dir;
  unsigned long max_memory;
  int workers;
  bool remove_duplicates;
};

parsed_options parse_cmline(int argc, char **argv);
unsigned long get_mem_total();

struct Comparator {
  bool operator()(const light_string &lhs, const light_string &rhs) {
    return lhs < rhs;
  }
};

int main(int argc, char **argv) {
  auto parsed = parse_cmline(argc, argv);

  std::cout << "given options:\n"
            << "workers: " << parsed.workers << "\n"
            << "max-memory: " << parsed.max_memory << "\n"
            << "tmp-dir: " << parsed.tmp_dir << std::endl;

  std::ifstream ifs(parsed.input_file, std::ios::in);
  std::ofstream ofs(parsed.output_file, std::ios::out | std::ios::trunc);
  external_sort(parsed.input_file, parsed.output_file, parsed.tmp_dir,
                parsed.workers, 10, parsed.max_memory, 4096,
                parsed.remove_duplicates, Comparator());
}

parsed_options parse_cmline(int argc, char **argv) {
  const char short_options[] = "i:o:t::m::w::u::";
  struct option long_options[] = {
      {"input-file", required_argument, nullptr, 'i'},
      {"output-file", required_argument, nullptr, 'o'},
      {"tmp-dir", optional_argument, nullptr, 't'},
      {"max-memory", optional_argument, nullptr, 'm'},
      {"workers", optional_argument, nullptr, 'w'},
      {"unique-values", optional_argument, nullptr, 'u'},
  };

  int opt, opt_index;

  bool has_input = false;
  bool has_output = false;
  bool has_tmp_dir = false;
  bool has_max_mem = false;
  bool has_workers = false;
  parsed_options out{};

  while ((
      opt = getopt_long(argc, argv, short_options, long_options, &opt_index))) {
    if (opt == -1) {
      break;
    }
    switch (opt) {
    case 'i':
      out.input_file = optarg;
      has_input = true;
      break;
    case 'o':
      out.output_file = optarg;
      has_output = optarg;
      break;
    case 't':
      if (optarg) {
        out.tmp_dir = optarg;
        has_tmp_dir = true;
      }
      break;
    case 'm':
      if (optarg) {
        out.max_memory = std::stoul(std::string(optarg));
        has_max_mem = true;
      }
      break;
    case 'w':
      if (optarg) {
        out.workers = std::stoi(std::string(optarg));
        has_workers = true;
      }
      break;
    case 'u':
      out.remove_duplicates = true;
      break;
    default:
      break;
    }
  }

  if (!has_input)
    throw std::runtime_error("input-file (i) argument is required");
  if (!has_output)
    throw std::runtime_error("output-file (o) argument is required");
  if (!has_tmp_dir) {
    auto tmp_base = std::filesystem::temp_directory_path();
    auto fname_template = (std::filesystem::path(tmp_base) /
                           std::filesystem::path("tmpsort_XXXXXX"))
                              .string();
    auto mut_fname_template =
        std::make_unique<char[]>(fname_template.size() + 1);
    std::copy(fname_template.begin(), fname_template.end(),
              mut_fname_template.get());
    mut_fname_template[fname_template.size()] = '\0';
    char *tmp_dir = mkdtemp(mut_fname_template.get());
    if (!tmp_dir)
      throw std::runtime_error("Couldn't generate tmp dir");
    out.tmp_dir = tmp_dir;
  }

  if (!has_max_mem) {
    auto mem_total = get_mem_total();
    if (mem_total > 0) {
      out.max_memory = mem_total / 2;
    } else {
      out.max_memory = 1'000'000'000; // 1GB
    }
  }

  if (!has_workers) {
    out.workers = 1;
  }
  return out;
}

unsigned long get_mem_total() {
  std::string token;
  std::ifstream file("/proc/meminfo");
  while (file >> token) {
    if (token == "MemTotal:") {
      unsigned long mem;
      if (file >> mem) {
        return mem;
      } else {
        return 0;
      }
    }
    // ignore rest of the line
    file.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
  }
  return 0; // nothing found
}
