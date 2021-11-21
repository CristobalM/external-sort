//
// Created by cristobal on 21-11-21.
//
#include <gtest/gtest.h>

#include <ULHeaderIOHandler.hpp>
#include <UnsignedLongSortConnectorWHeader.hpp>
#include <external_sort.hpp>
#include <fstream>
#include <string>

static void write_ul(std::ofstream &ofs, unsigned long value) {
  ofs.write(reinterpret_cast<char *>(&value), sizeof(unsigned long));
}

static unsigned long read_ul(std::ifstream &ifs) {
  unsigned long result;

  ifs.read(reinterpret_cast<char *>(&result), sizeof(unsigned long));

  return result;
}

TEST(IOHandlerWHeader, works_on_files_wheader) {
  const std::string ul_data("works_on_files_wheader.bin");
  const std::string sorted_ul_data("works_on_files_wheader.sorted.bin");
  const std::string tmp_dir("./");

  const auto sz = 10'000'000L;
  {
    std::ofstream ofs(ul_data,
                      std::ios::binary | std::ios::out | std::ios::trunc);
    write_ul(ofs, sz);

    for (long i = sz - 1; i > -1L; i--) {
      write_ul(ofs, i);
    }
  }

  ExternalSort::ExternalSort<
      ExternalSort::UnsignedLongSortConnectorWHeader, ExternalSort::BINARY,
      ExternalSort::NoTimeControl,
      ExternalSort::ULHeaderIOHandler>::sort(ul_data, sorted_ul_data, tmp_dir,
                                             1, 10, 1'000'000'000, 4096, false);

  std::ifstream ifs(sorted_ul_data, std::ios::in | std::ios::binary);

  auto extracted_sz = read_ul(ifs);
  ASSERT_EQ(extracted_sz, sz);
  for (size_t i = 0; i < sz; i++) {
    auto value = read_ul(ifs);
    ASSERT_EQ(value, i);
  }
}