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
#include <sstream>
#include <string>
#include <vector>
#include <unordered_set>

#include "quicksort.hpp"

#include "ParallelWorker.hpp"
#include "light_string.hpp"

using pair_string_int = std::pair<light_string, int>;

template <typename Comparator> struct PairComp {
  bool operator()(const pair_string_int &lhs, const pair_string_int &rhs) {
    return !Comparator()(lhs.first, rhs.first);
  }
};

template <typename Comparator>
static void parallel_sort(std::vector<light_string> &data, int max_workers,
                          unsigned long segment_size,
                          Comparator &comparator) {

  std::vector<int> offsets = {0};
  std::unordered_set<int> offsets_set;
  unsigned long acc_size = data[0].size();
  for(int i = 1; i < static_cast<int>(data.size()); i++ ){
    acc_size += data[i].size();
    if(acc_size >= segment_size){
      offsets.push_back(i);
      offsets_set.insert(i);
      acc_size = 0;
    }
  }
  offsets.push_back(data.size());
  offsets_set.insert(data.size());

  int parts = offsets.size() -1;
  int workers = std::min(max_workers, parts);
  if (workers == 1) {
    std::sort(data.begin(), data.end(), comparator);
    return;
  }

  ParallelWorkerPool pool(workers);

  for(int i = 0; i < parts; i++){
    pool.add_task([i, &offsets, &data, &comparator](){
      std::sort(data.begin()+offsets[i], data.begin() + offsets[i+1], comparator);
    });
  }

  pool.stop_all_workers();
  pool.wait_workers();

  std::vector<light_string> result;

  std::priority_queue<pair_string_int, std::vector<pair_string_int>, PairComp<Comparator>> pqueue;

  for(int i = 0; i < parts; i++){
    pqueue.push({data[offsets[i]], offsets[i]});
  }

  while(!pqueue.empty()){
    auto &current = pqueue.top();
    result.push_back(current.first);
    int next = current.second + 1;
    pqueue.pop();
    if(offsets_set.find(next) != offsets_set.end()){
      continue;
    }
    pqueue.push({data[next], next});
  }

  data = std::move(result);
  
}

template <typename Comparator>
static void create_file_part(
  const std::string &input_filename_base,
   const std::string &tmp_dir,
  int workers, // workers,
  std::vector<char> &buffer_out,
  Comparator &comparator,
  unsigned long &accumulated_size,
  std::vector<light_string> &data,
  int &current_file_index,
  std::vector<std::string> &filenames
){
  accumulated_size = 0;
  auto filename =
      (std::filesystem::path(tmp_dir) /
        std::filesystem::path(input_filename_base + "-p" +
                              std::to_string(current_file_index++)))
          .string();
  std::ofstream ofs(filename, std::ios::out);
  ofs.rdbuf()->pubsetbuf(buffer_out.data(), buffer_out.size());
  filenames.push_back(filename);
  // std::sort(data.begin(), data.end(), comparator);
  parallel_sort(data, workers, 1'000'000'000, comparator);
  for (auto &line : data) {
    ofs << line << '\n';
  }
  data.clear();
}

template <typename Comparator>
static std::vector<std::string>
split_file(const std::string &input_filename, const std::string &tmp_dir,
           unsigned long memory_budget,
           int workers,
           std::vector<char> &buffer_in, std::vector<char> &buffer_out,
           Comparator &comparator) {

  std::vector<std::string> filenames;
  std::ifstream input_file(input_filename, std::ios::in);

  input_file.rdbuf()->pubsetbuf(buffer_in.data(), buffer_in.size());

  int current_file_index = 0;
  unsigned long accumulated_size = 0;

  std::vector<light_string> data;

  std::string line;
  while (std::getline(input_file, line)) {
    if (accumulated_size >= memory_budget/3) {
      create_file_part(input_filename, tmp_dir, workers, buffer_out, comparator, accumulated_size, data, current_file_index, filenames);
    }
    data.push_back(light_string(line));
    accumulated_size += (line.size() + 1) * sizeof(char) +
                        sizeof(light_string) + sizeof(light_string *);
  }
  if(accumulated_size > 0)
    create_file_part(input_filename, tmp_dir, workers, buffer_out, comparator, accumulated_size, data, current_file_index, filenames);
  return filenames;
}

template <typename Comparator>
static void sort_file(const std::string &filename,
                      int, // workers add later
                      std::vector<char> &buffer_in,
                      std::vector<char> &buffer_out, Comparator &comparator) {
  std::vector<light_string> data;

  std::ifstream ifs(filename, std::ios::in);
  ifs.rdbuf()->pubsetbuf(buffer_in.data(), buffer_in.size());
  std::string line;
  while (std::getline(ifs, line)) {
    data.push_back(light_string(line));
  }
  ifs.close();
  std::sort(data.begin(), data.end(), comparator);
  /// inplace_quicksort(data, comparator);

  std::ofstream ofs(filename, std::ios::out | std::ios::trunc);
  ofs.rdbuf()->pubsetbuf(buffer_out.data(), buffer_out.size());
  for (auto &item : data) {
    ofs << item << '\n';
  }
  ofs.flush();
  ofs.close();
}

template <typename Comparator>
static void sort_files(const std::vector<std::string> &filenames, int workers,
                       std::vector<char> &buffer_in,
                       std::vector<char> &buffer_out, Comparator &comparator) {
  for (const auto &file : filenames) {
    sort_file(file, workers, buffer_in, buffer_out, comparator);
  }
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

static bool fill_with_file(std::list<light_string> &data_block,
                           std::unique_ptr<std::ifstream> &input_file,
                           unsigned long block_size) {
  std::string line;
  unsigned long accumulated_size = 0;
  while (accumulated_size < block_size) {
    if (!std::getline(*input_file, line)) {
      input_file = nullptr;
      break;
    }
    accumulated_size += (line.size() + 1) * sizeof(char) +
                        sizeof(light_string) + sizeof(light_string *);
    data_block.push_back(light_string(line));
  }
  return !data_block.empty();
}



template <typename Comparator>
static void
block_update(int index, std::vector<std::list<light_string>> &data,
             std::vector<std::unique_ptr<std::ifstream>> &opened_files,
             std::priority_queue<pair_string_int, std::vector<pair_string_int>,
                                 PairComp<Comparator>> &pqueue,
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

template <typename Comparator>
static std::string
merge_pass(const std::vector<std::string> &filenames, int start, int end,
           const std::string &tmp_dir, unsigned long block_size,
           std::vector<std::vector<char>> &buffers, Comparator &) {

  std::vector<std::unique_ptr<std::ifstream>> opened_files;

  auto result_template = (std::filesystem::path(tmp_dir) / "m_XXXXXX").string();
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
  std::ofstream ofs(result_filename, std::ios::out);

  ofs.rdbuf()->pubsetbuf(buffers[buffers.size() - 1].data(),
                         buffers[buffers.size() - 1].size());

  std::priority_queue<pair_string_int, std::vector<pair_string_int>,
                      PairComp<Comparator>>
      pqueue;

  for (int i = start; i < end; i++) {
    opened_files.push_back(
        std::make_unique<std::ifstream>(filenames[i], std::ios::in));

    opened_files.back()->rdbuf()->pubsetbuf(buffers[i - start].data(),
                                            buffers[i - start].size());
  }

  std::vector<std::list<light_string>> data;

  std::string line;
  for (auto &file : opened_files) {
    unsigned long accumulated_size = 0;

    std::list<light_string> current_block;
    while (accumulated_size < block_size) {
      if (!std::getline(*file, line)) {
        file = nullptr;
        break;
      }
      accumulated_size += (line.size() + 1) * sizeof(char) +
                          sizeof(light_string) + sizeof(light_string *);
      current_block.push_back(light_string(line));
    }
    data.push_back(std::move(current_block));
  }

  for (int i = 0; i < static_cast<int>(data.size()); i++) {
    block_update(i, data, opened_files, pqueue, block_size);
  }

  while (!pqueue.empty()) {
    auto &current = pqueue.top();
    int index = current.second;
    ofs << current.first << '\n';
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

template <typename Comparator>
static std::vector<std::string> merge_bottom_up(
    const std::vector<std::string> &filenames, const std::string &tmp_dir,
    int max_files, unsigned long block_size,
    std::vector<std::vector<char>> &buffers, Comparator &comparator) {
  std::vector<std::string> result_filenames;

  int level_passes = (filenames.size() / max_files) +
                     (filenames.size() % max_files != 0 ? 1 : 0);
  for (int current_pass = 0; current_pass < level_passes; current_pass++) {
    auto pass_file = merge_pass(
        filenames, current_pass * max_files,
        std::min<int>((current_pass + 1) * max_files, filenames.size()),
        tmp_dir, block_size, buffers, comparator);
    result_filenames.push_back(pass_file);
  }

  return result_filenames;
}

static std::vector<std::vector<char>> init_buffers(int max_files,
                                                   unsigned long block_size) {
  std::vector<std::vector<char>> buffers;
  for (int i = 0; i < max_files + 1; i++) {
    buffers.push_back(std::vector<char>(block_size));
  }
  return buffers;
}

template <typename Comparator>
void external_sort(const std::string &input_filename,
                   const std::string &output_filename,
                   const std::string &tmp_dir, int workers, int max_files,
                   unsigned long memory_budget, unsigned long block_size,
                   Comparator comparator) {

  auto buffers = init_buffers(max_files, block_size);

  auto current_filenames =
      split_file(input_filename, tmp_dir, memory_budget, workers, buffers[0],
                 buffers[max_files], comparator);

  /*
  std::cout << "started sorting files" << std::endl;
  sort_files(current_filenames, workers, buffers[0], buffers[max_files],
  comparator); std::cout << "finished sorting files" << std::endl;
  */
  while (current_filenames.size() > 1) {
    current_filenames = merge_bottom_up(current_filenames, tmp_dir, max_files,
                                        block_size, buffers, comparator);
  }
  rename(current_filenames[0].c_str(), output_filename.c_str());
}

#endif /* _EXTERNAL_SORT_HPP_ */