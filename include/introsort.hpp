#ifndef _ES_QUICK_SORT_HPP_
#define _ES_QUICK_SORT_HPP_

#include "time_control.hpp"
#include <algorithm>
#include <random>
#include <vector>

namespace ExternalSort {

template <typename T, typename TC = NoTimeControl> class IntroSort {
public:
  using comp_t = typename T::Comparator;

  static void sort(std::vector<T> &data, comp_t &comparator) {
    TC tc;
    int max_depth = 2 * std::log2(data.size());
    introsort_rec(data, max_depth, comparator, 0, data.size(), tc, 0);
  }

  static void sort(std::vector<T> &data, comp_t &comparator, TC &time_control) {
    int max_depth = 2 * std::log2(data.size());
    introsort_rec(data, max_depth, comparator, 0, data.size(), time_control, 0);
  }

  static void sort(std::vector<T> &data, comp_t &comparator, TC &time_control,
                   int start, int end) {
    int max_depth = 2 * std::log2(data.size());
    introsort_rec(data, max_depth, comparator, start, end, time_control, 0);
  }
  static void sort(std::vector<T> &data, comp_t &comparator, int start,
                   int end) {
    int max_depth = 2 * std::log2(data.size());
    TC tc;
    introsort_rec(data, max_depth, comparator, start, end, tc, 0);
  }

private:
  static int quicksort_partition(std::vector<T> &data, comp_t &comparator,
                                 int start, int end, TC &time_control) {
    int left = start - 1;
    auto &pivot_value = data[end - 1];
    for (int right = start; right < end - 1; right++) {
      if constexpr (TC::with_time_control) {
        if (!time_control.tick())
          return 0;
      }
      if (comparator(data[right], pivot_value)) {
        left++;
        std::swap(data[left], data[right]);
      }
    }
    std::swap(pivot_value, data[left + 1]);
    return left + 1;
  }

  static int
  quicksort_partition_random(std::vector<T> &data, comp_t &comparator,
                             int start, int end,
                             std::uniform_int_distribution<int> &random_distr,
                             std::mt19937 &generator, TC &time_control) {
    int pivot = (random_distr(generator) % (end - start)) + start;
    std::swap(data[pivot], data[end - 1]);
    return quicksort_partition(data, comparator, start, end, time_control);
  }

  static void introsort_with_distr(
      std::vector<T> &data, int max_depth, comp_t &comparator, int start,
      int end, std::uniform_int_distribution<int> &random_distr,
      std::mt19937 &generator, TC &time_control, int current_depth) {
    if (start >= end)
      return;
    if constexpr (TC::with_time_control) {
      if (!time_control.tick())
        return;
    }

    int partition_length = end - start;
    if (partition_length < 16) {
      insertion_sort(data, comparator, start, end);
      return;
    }

    if (current_depth >= max_depth) {
      heap_sort(data, comparator, start, end, time_control);
      return;
    }

    int pivot = quicksort_partition_random(
        data, comparator, start, end, random_distr, generator, time_control);

    if constexpr (TC::with_time_control) {
      if (!time_control.tick())
        return;
    }

    introsort_with_distr(data, max_depth, comparator, start, pivot,
                         random_distr, generator, time_control,
                         current_depth + 1);
    if constexpr (TC::with_time_control) {
      if (!time_control.tick())
        return;
    }

    introsort_with_distr(data, max_depth, comparator, pivot + 1, end,
                         random_distr, generator, time_control,
                         current_depth + 1);
    if constexpr (TC::with_time_control) {
      if (!time_control.tick())
        return;
    }
  }

  static void introsort_rec(std::vector<T> &data, int max_depth,
                            comp_t &comparator, int start, int end,
                            TC &time_control, int current_depth) {
    static std::random_device random_device;
    static std::mt19937 generator(random_device());
    static std::uniform_int_distribution random_distr(start, end);
    return introsort_with_distr(data, max_depth, comparator, start, end,
                                random_distr, generator, time_control,
                                current_depth);
  }

  static void insertion_sort(std::vector<T> &data, comp_t &comparator,
                             int start, int end) {
    for (int j = start; j < end; j++) {
      auto key = std::move(data[j]);
      int i = j - 1;
      while (i >= 0 && comparator(key, data[i])) {
        std::swap(data[i + 1], data[i]);
        i--;
      }
      data[i + 1] = std::move(key);
    }
  }

  static void heap_sort(std::vector<T> &data, comp_t &comparator, int start,
                        int end, TC &time_control) {
    build_max_heap(data, comparator, start, end - start, time_control);
    if constexpr (TC::with_time_control)
      if (!time_control.tick())
        return;
    for (int i = end - 1; i >= start + 1; i--) {
      if constexpr (TC::with_time_control)
        if (!time_control.tick())
          return;
      std::swap(data[start], data[i]);
      heapify(data, comparator, start, start, i - start, time_control);
    }
  }

  static void build_max_heap(std::vector<T> &data, comp_t &comparator,
                             int start, int heap_size, TC &time_control) {
    for (int i = ((heap_size - 2) / 2); i >= 0; i--) {
      heapify(data, comparator, start, i + start, heap_size, time_control);
      if constexpr (TC::with_time_control)
        if (!time_control.tick())
          return;
    }
  }

  static inline int left(int pos, int start) {
    int local_pos = pos - start;
    return (local_pos << 1) + start + 1;
  }
  static inline int right(int pos, int start) {
    int local_pos = pos - start;
    return (local_pos << 1) + start + 2;
  }
  static void heapify(std::vector<T> &data, comp_t &comparator, int start,
                      int pos, int heap_size, TC &time_control) {
    while (pos < heap_size + start) {
      if constexpr (TC::with_time_control)
        if (!time_control.tick())
          return;
      int l = left(pos, start);
      int r = right(pos, start);
      int max_val_pos;
      if (l < heap_size + start && comparator(data[pos], data[l]))
        max_val_pos = l;
      else
        max_val_pos = pos;
      if (r < heap_size + start && comparator(data[max_val_pos], data[r]))
        max_val_pos = r;
      if (max_val_pos != pos) {
        std::swap(data[pos], data[max_val_pos]);
        pos = max_val_pos;
      } else
        break;
    }
  }
};

} // namespace ExternalSort

#endif /* _ES_QUICK_SORT_HPP_ */