#include <gtest/gtest.h>

#include <introsort.hpp>

#include <chrono>
#include <string>

struct IntAdapter {
  int value;
  explicit IntAdapter(int value) : value(value) {}
  struct Comparator {
    bool operator()(const IntAdapter &lhs, const IntAdapter &rhs) {
      return lhs.value < rhs.value;
    }
  };
};

struct StringAdapter {
  std::string value;
  explicit StringAdapter(std::string value) : value(std::move(value)) {}
  struct Comparator {
    bool operator()(const StringAdapter &lhs, const StringAdapter &rhs) {
      return lhs.value < rhs.value;
    }
  };
};

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
  std::vector<IntAdapter> data;

  int max_value = 1000000;

  for (int i = max_value; i >= 0; i--) {
    data.emplace_back(i);
  }

  auto data2 = data;

  auto begin_2 = std::chrono::steady_clock::now();
  std::sort(data2.begin(), data2.end(), IntAdapter::Comparator());
  auto end_2 = std::chrono::steady_clock::now();

  IntAdapter::Comparator comp;
  auto begin_1 = std::chrono::steady_clock::now();
  ExternalSort::IntroSort<IntAdapter>::sort(data, comp);
  // ExternalSort::IntroSort<IntAdapter>::heap_sort(data, comp);
  // ExternalSort::IntroSort<IntAdapter>::insertion_sort(data, comp);
  auto end_1 = std::chrono::steady_clock::now();

  for (int i = 0; i <= max_value; i++) {
    ASSERT_EQ(data[i].value, i);
  }

  std::cout << "First = "
            << std::chrono::duration_cast<std::chrono::milliseconds>(end_1 -
                                                                     begin_1)
                   .count()
            << "[ms]" << std::endl;
  std::cout << "Second = "
            << std::chrono::duration_cast<std::chrono::milliseconds>(end_2 -
                                                                     begin_2)
                   .count()
            << "[ms]" << std::endl;
}

TEST(inplace_quicksort, test_quick_sort_1_strings) {
  std::vector<StringAdapter> data;
  for (int i = 100000; i >= 0; i--) {
    data.emplace_back(transform_int_to_str_padded(i, 10));
  }

  StringAdapter::Comparator comp;
  ExternalSort::IntroSort<StringAdapter>::sort(data, comp);

  for (int i = 0; i <= 100000; i++) {
    ASSERT_EQ(data[i].value, transform_int_to_str_padded(i, 10));
  }
}
