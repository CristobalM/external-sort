#include <gtest/gtest.h>

#include <quicksort.hpp>

static std::string transform_int_to_str_padded(unsigned long value,
                                               unsigned long padding) {
  std::stringstream ss;

  unsigned long max_val = std::pow(10, padding) - 1;
  if (value > max_val)
    throw std::runtime_error("value " + std::to_string(value) +
                             " is bigger than max_value " +
                             std::to_string(max_val));

  unsigned long next_pow10 = std::ceil(std::log10(value + 1));
  unsigned long remaining_zeros = padding - next_pow10;
  for (unsigned long i = 0; i < remaining_zeros; i++)
    ss << "0";
  if (value != 0)
    ss << std::to_string(value);
  return ss.str();
}

TEST(inplace_quicksort, test_quick_sort_1_int) {
  std::vector<int> data;
  for (int i = 100000; i >= 0; i--) {
    data.push_back(i);
  }

  inplace_quicksort(data, [](int lhs, int rhs) { return lhs <= rhs; });

  for (int i = 0; i <= 100000; i++) {
    ASSERT_EQ(data[i], i);
  }
}

TEST(inplace_quicksort, test_builtin_sort_1_int) {
  std::vector<int> data;
  for (int i = 100000; i >= 0; i--) {
    data.push_back(i);
  }

  std::sort(data.begin(), data.end(),
            [](int lhs, int rhs) { return lhs <= rhs; });

  for (int i = 0; i <= 100000; i++) {
    ASSERT_EQ(data[i], i);
  }
}

TEST(inplace_quicksort, test_quick_sort_1_strings) {
  std::vector<std::string> data;
  for (int i = 100000; i >= 0; i--) {
    data.push_back(transform_int_to_str_padded(i, 10));
  }

  inplace_quicksort(data, [](const std::string &lhs, const std::string &rhs) {
    return lhs <= rhs;
  });

  for (int i = 0; i <= 100000; i++) {
    ASSERT_EQ(data[i], transform_int_to_str_padded(i, 10));
  }
}
TEST(inplace_quicksort, test_builtin_sort_1_strings) {
  std::vector<std::string> data;
  for (int i = 100000; i >= 0; i--) {
    data.push_back(transform_int_to_str_padded(i, 10));
  }

  std::sort(data.begin(), data.end(),
            [](const std::string &lhs, const std::string &rhs) {
              return lhs <= rhs;
            });

  for (int i = 0; i <= 100000; i++) {
    ASSERT_EQ(data[i], transform_int_to_str_padded(i, 10));
  }
}
