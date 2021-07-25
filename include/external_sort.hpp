#ifndef _EXTERNAL_SORT_HPP_
#define _EXTERNAL_SORT_HPP_

#include <algorithm>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <list>
#include <memory>
#include <queue>
#include <regex>
#include <set>
#include <sstream>
#include <string>
#include <unordered_set>
#include <vector>

#include "quicksort.hpp"

#include "ParallelWorker.hpp"

namespace ExternalSort {

enum DATA_MODE { BINARY = 0, TEXT = 1 };

template <typename T, DATA_MODE DM = TEXT> class ExternalSort {
  using pair_T_int = std::pair<T, int>;

  struct PairComp {
    bool operator()(const pair_T_int &lhs, const pair_T_int &rhs) {
      return !(typename T::Comparator()(lhs.first, rhs.first));
    }
  };

public:
  static void sort(const std::string &input_filename,
                   const std::string &output_filename,
                   const std::string &tmp_dir, int workers, int max_files,
                   unsigned long memory_budget, unsigned long block_size,
                   bool remove_duplicates) {

    auto buffers = init_buffers(max_files, block_size);

    auto current_filenames =
        split_file(input_filename, tmp_dir, memory_budget, workers, buffers[0],
                   buffers[max_files], remove_duplicates);

    while (current_filenames.size() > 1) {
      current_filenames =
          merge_bottom_up(current_filenames, tmp_dir, max_files, block_size,
                          buffers, remove_duplicates);
    }
    auto from = std::filesystem::path(current_filenames[0]);
    auto to = std::filesystem::path(output_filename);

    try {
      std::filesystem::rename(from, to);
    } catch (const std::filesystem::filesystem_error &e) {
      if (std::filesystem::exists(to)) {
        std::filesystem::remove(to);
      }
      std::filesystem::copy_file(from, to);
      std::filesystem::remove(from);
    }
  }

private:
  static void parallel_sort(std::vector<T> &data, int max_workers,
                            unsigned long segment_size,
                            bool remove_duplicates) {

    std::vector<int> offsets = {0};
    std::unordered_set<int> offsets_set;
    unsigned long accumulated_size = data[0].size();
    for (int i = 1; i < static_cast<int>(data.size()); i++) {
      accumulated_size += data[i].size();
      if (accumulated_size >= segment_size) {
        offsets.push_back(i);
        offsets_set.insert(i);
        accumulated_size = 0;
      }
    }
    offsets.push_back(data.size());
    offsets_set.insert(data.size());

    int parts = static_cast<int>(offsets.size()) - 1;
    int workers = std::min(max_workers, parts);
    if (workers == 1) {
      std::sort(data.begin(), data.end(), typename T::Comparator());
      if (remove_duplicates)
        data.erase(std::unique(data.begin(), data.end()), data.end());

      return;
    }

    ParallelWorkerPool pool(workers);

    for (int i = 0; i < parts; i++) {
      pool.add_task([i, &offsets, &data]() {
        std::sort(data.begin() + offsets[i], data.begin() + offsets[i + 1],
                  typename T::Comparator());
      });
    }

    pool.stop_all_workers();
    pool.wait_workers();

    std::vector<T> result;

    std::priority_queue<pair_T_int, std::vector<pair_T_int>, PairComp> pqueue;

    for (int i = 0; i < parts; i++) {
      pqueue.push({data[offsets[i]], offsets[i]});
    }

    while (!pqueue.empty()) {
      auto &current = pqueue.top();
      result.push_back(current.first);
      int next = current.second + 1;
      pqueue.pop();
      if (offsets_set.find(next) != offsets_set.end()) {
        continue;
      }
      pqueue.push({data[next], next});
    }

    data = std::move(result);
    if (remove_duplicates)
      data.erase(std::unique(data.begin(), data.end()), data.end());
  }

  static void create_file_part(const std::string &input_filename_base,
                               const std::string &tmp_dir, int workers,
                               std::vector<char> &buffer_out,
                               unsigned long &accumulated_size,
                               std::vector<T> &data, int &current_file_index,
                               std::vector<std::string> &filenames,
                               bool remove_duplicates) {
    accumulated_size = 0;
    auto filename =
        (std::filesystem::path(tmp_dir) /
         std::filesystem::path(input_filename_base + "-p" +
                               std::to_string(current_file_index++)))
            .string();

    std::ios_base::openmode open_mode;
    if constexpr (DM == TEXT) {
      open_mode = std::ios::out;
    } else {
      open_mode = std::ios::out | std::ios::binary;
    }

    std::ofstream ofs(filename, open_mode);
    ofs.rdbuf()->pubsetbuf(buffer_out.data(),
                           static_cast<std::streamsize>(buffer_out.size()));
    filenames.push_back(filename);
    parallel_sort(data, workers, 1'000'000'000, remove_duplicates);
    for (auto &line : data) {
      ofs << line;
    }
    data.clear();
  }

  static std::vector<std::string>
  split_file(const std::string &input_filename, const std::string &tmp_dir,
             unsigned long memory_budget, int workers,
             std::vector<char> &buffer_in, std::vector<char> &buffer_out,
             bool remove_duplicates) {

    std::vector<std::string> filenames;

    std::ios_base::openmode open_mode;
    if constexpr (DM == TEXT) {
      open_mode = std::ios::in;
    } else {
      open_mode = std::ios::in | std::ios::binary;
    }
    std::ifstream input_file(input_filename, open_mode);

    input_file.rdbuf()->pubsetbuf(
        buffer_in.data(), static_cast<std::streamsize>(buffer_in.size()));

    int current_file_index = 0;
    unsigned long accumulated_size = 0;

    std::vector<T> data;

    // std::string line;
    T current_val;
    while (T::read_value(input_file, current_val)) {
      if (accumulated_size >= memory_budget / 3) {
        create_file_part(input_filename, tmp_dir, workers, buffer_out,
                         accumulated_size, data, current_file_index, filenames,
                         remove_duplicates);
      }
      data.push_back(current_val);
      accumulated_size +=
          (current_val.size() + 1) * sizeof(char) + sizeof(T) + sizeof(T *);
    }
    if (accumulated_size > 0)
      create_file_part(input_filename, tmp_dir, workers, buffer_out,
                       accumulated_size, data, current_file_index, filenames,
                       remove_duplicates);
    return filenames;
  }

  std::string concatenate_filenames(const std::vector<std::string> &filenames) {
    std::stringstream ss;
    for (const auto &filename : filenames) {
      auto filename_relative =
          std::filesystem::path(filename).filename().string();
      // std::replace(filename_relative.begin(), filename_relative.end(), '/',
      // ''):
      ss << "_" << filename_relative;
    }
    return ss.str();
  }

  static bool fill_with_file(std::list<T> &data_block,
                             std::unique_ptr<std::ifstream> &input_file,
                             unsigned long block_size) {

    T current_value;
    unsigned long accumulated_size = 0;
    while (accumulated_size < block_size) {
      if (!T::read_value(*input_file, current_value)) {
        input_file = nullptr;
        break;
      }
      accumulated_size +=
          (current_value.size() + 1) * sizeof(char) + sizeof(T) + sizeof(T *);
      data_block.push_back(current_value);
    }
    return !data_block.empty();
  }

  static void block_update(
      int index, std::vector<std::list<T>> &data,
      std::vector<std::unique_ptr<std::ifstream>> &opened_files,
      std::priority_queue<pair_T_int, std::vector<pair_T_int>, PairComp>
          &pqueue,
      unsigned long block_size) {
    if (data[index].empty() && opened_files[index]) {
      if (!fill_with_file(data[index], opened_files[index], block_size))
        return;
    } else if (data[index].empty()) {
      return;
    }
    auto current_str = data[index].front();
    pqueue.push({std::move(current_str), index});
    data[index].pop_front();
  }

  static std::string merge_pass(const std::vector<std::string> &filenames,
                                int start, int end, const std::string &tmp_dir,
                                unsigned long block_size,
                                std::vector<std::vector<char>> &buffers,
                                bool remove_duplicates) {

    std::vector<std::unique_ptr<std::ifstream>> opened_files;

    auto result_template =
        (std::filesystem::path(tmp_dir) / "m_XXXXXX").string();
    auto mut_fname_template = std::vector<char>(result_template.size() + 1);
    std::copy(result_template.begin(), result_template.end(),
              mut_fname_template.data());
    mut_fname_template[result_template.size()] = '\0';
    int created = mkstemp(mut_fname_template.data());
    auto result_filename =
        std::string(mut_fname_template.begin(), mut_fname_template.end());
    if (!created)
      throw std::runtime_error("couldn't generate tmp file with name " +
                               result_filename);
    std::ios_base::openmode open_mode;
    if constexpr (DM == TEXT) {
      open_mode = std::ios::out;
    } else {
      open_mode = std::ios::out | std::ios::binary;
    }
    std::ofstream ofs(result_filename, open_mode);

    ofs.rdbuf()->pubsetbuf(
        buffers[buffers.size() - 1].data(),
        static_cast<std::streamsize>(buffers[buffers.size() - 1].size()));

    std::priority_queue<pair_T_int, std::vector<pair_T_int>, PairComp> pqueue;

    for (int i = start; i < end; i++) {

      opened_files.push_back(
          std::make_unique<std::ifstream>(filenames[i], open_mode));

      opened_files.back()->rdbuf()->pubsetbuf(
          buffers[i - start].data(),
          static_cast<std::streamsize>(buffers[i - start].size()));
    }

    std::vector<std::list<T>> data;

    T current_value;
    for (auto &file : opened_files) {
      unsigned long accumulated_size = 0;

      std::list<T> current_block;
      while (accumulated_size < block_size) {
        if (!T::read_value(*file, current_value)) {
          file = nullptr;
          break;
        }
        accumulated_size +=
            (current_value.size() + 1) * sizeof(char) + sizeof(T) + sizeof(T *);
        current_block.push_back(current_value);
      }
      data.push_back(std::move(current_block));
    }

    for (int i = 0; i < static_cast<int>(data.size()); i++) {
      block_update(i, data, opened_files, pqueue, block_size);
    }

    T last_value;
    while (!pqueue.empty()) {
      auto &current = pqueue.top();
      int index = current.second;
      if (!remove_duplicates || (last_value != current.first)) {
        ofs << current.first;
      }

      last_value = current.first;
      pqueue.pop();
      block_update(index, data, opened_files, pqueue, block_size);
    }

    ofs.flush();
    ofs.close();

    for (int i = start; i < end; i++) {
      remove(filenames.at(i).c_str());
    }

    return result_filename;
  }

  static std::vector<std::string> merge_bottom_up(
      const std::vector<std::string> &filenames, const std::string &tmp_dir,
      int max_files, unsigned long block_size,
      std::vector<std::vector<char>> &buffers, bool remove_duplicates) {
    std::vector<std::string> result_filenames;

    int level_passes = static_cast<int>(filenames.size() / max_files) +
                       (filenames.size() % max_files != 0 ? 1 : 0);
    for (int current_pass = 0; current_pass < level_passes; current_pass++) {
      auto pass_file =
          merge_pass(filenames, current_pass * max_files,
                     std::min<int>((current_pass + 1) * max_files,
                                   static_cast<int>(filenames.size())),
                     tmp_dir, block_size, buffers, remove_duplicates);
      result_filenames.push_back(pass_file);
    }

    return result_filenames;
  }

  static std::vector<std::vector<char>> init_buffers(int max_files,
                                                     unsigned long block_size) {
    std::vector<std::vector<char>> buffers;
    buffers.reserve(max_files + 1);
    for (int i = 0; i < max_files + 1; i++) {
      buffers.emplace_back(block_size);
    }
    return buffers;
  }
};

} // namespace ExternalSort

#endif /* _EXTERNAL_SORT_HPP_ */