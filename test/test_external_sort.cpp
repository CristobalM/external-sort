#include <gtest/gtest.h>

#include <sstream>
#include <cmath>
#include <fstream>
#include <external_sort.hpp>

#include <light_string.hpp>


static std::string transform_int_to_str_padded(int value, int padding) {
  std::stringstream ss;

  int max_val = std::pow(10, padding) - 1;
  if (value > max_val)
    throw std::runtime_error("value " + std::to_string(value) +
                             " is bigger than max_value " +
                             std::to_string(max_val));

  int next_pow10 = std::ceil(std::log10(value + 1));
  int remaining_zeros = padding - next_pow10;
  for (int i = 0; i < remaining_zeros; i++)
    ss << "0";
  if (value != 0)
    ss << std::to_string(value);
  return ss.str();
}

struct Comparator {
  
  bool operator()(const light_string &lhs, const light_string &rhs){
    return lhs < rhs;
  }
};

TEST(ExternalSortSuite, test_1){


  std::string debug_file_name("debug_file.txt");
  std::string output_file_name("output_file.txt");
  std::string tmp_dir("./");
  std::ofstream debug_file(debug_file_name, std::ios::out);


  for(int i = 0; i < 10'000'000; i++){
    debug_file << transform_int_to_str_padded(i, 9) << '\n';
  }
  debug_file.close();
  // external_sort_input_stream(ss, outputss, 1'000'000'000, 4, "./", Comparator());
  Comparator comparator;
  external_sort(debug_file_name, output_file_name, tmp_dir, 1, 10, 1'000'000, 500'000, comparator);
}
