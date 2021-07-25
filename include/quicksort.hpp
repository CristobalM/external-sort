#ifndef _ES_QUICK_SORT_HPP_
#define _ES_QUICK_SORT_HPP_

#include <algorithm>
#include <random>
#include <vector>

namespace {

template <typename T, typename Comparator>
static int inplace_quicksort_partition(std::vector<T> &data,
                                       Comparator &comparator, int start,
                                       int end) {
  int left = start - 1;
  auto &pivot_value = data[end - 1];
  for (int right = start; right < end - 1; right++) {
    if (comparator(data[right], pivot_value)) {
      left++;
      std::swap(data[left], data[right]);
    }
  }
  std::swap(pivot_value, data[left + 1]);
  return left + 1;
}

template <typename T, typename Comparator>
static int inplace_quicksort_partition_random(
    std::vector<T> &data, Comparator &comparator, int start, int end,
    std::uniform_int_distribution<int> &random_distr, std::mt19937 &generator) {
  int pivot = (random_distr(generator) % (end - start)) + start;
  std::swap(data[pivot], data[end - 1]);
  return inplace_quicksort_partition(data, comparator, start, end);
}

template <typename T, typename Comparator>
void inplace_quicksort(std::vector<T> &data, Comparator &comparator, int start,
                       int end,
                       std::uniform_int_distribution<int> &random_distr,
                       std::mt19937 &generator) {
  if (start >= end)
    return;

  int pivot = inplace_quicksort_partition_random(data, comparator, start, end,
                                                 random_distr, generator);
  inplace_quicksort(data, comparator, start, pivot, random_distr, generator);
  inplace_quicksort(data, comparator, pivot + 1, end, random_distr, generator);
}

} // namespace

template <typename T, typename Comparator>
void inplace_quicksort(std::vector<T> &data, Comparator &comparator, int start,
                       int end) {
  std::random_device random_device;
  std::mt19937 generator(random_device());
  std::uniform_int_distribution random_distr(start, end);
  return inplace_quicksort(data, comparator, start, end, random_distr,
                           generator);
}

template <typename T, typename Comparator>
void inplace_quicksort(std::vector<T> &data, Comparator comparator) {
  inplace_quicksort(data, comparator, 0, data.size());
}

#endif /* _ES_QUICK_SORT_HPP_ */