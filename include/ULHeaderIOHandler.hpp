//
// Created by cristobal on 21-11-21.
//

#ifndef EXTERNAL_SORT_ULHEADERIOHANDLER_HPP
#define EXTERNAL_SORT_ULHEADERIOHANDLER_HPP
namespace ExternalSort {

class ULHeaderIOHandler {
public:
  class Reader {
    std::ifstream &is;
    unsigned long counter;

    unsigned long sz;

  public:
    explicit Reader(std::ifstream &is) : is(is), counter(0), sz(0) {
      is.read(reinterpret_cast<char *>(&sz), sizeof(unsigned long));
    }

    template <typename T> bool read_value(T &out) {
      T::read_value(is, out);
      counter++;
      return counter <= sz;
    }
  };

  class Writer {
    std::ofstream &ofs;
    unsigned long elements_in_file;
    long header_pos;

  public:
    Writer(std::ofstream &ofs, unsigned long elements_in_file)
        : ofs(ofs), elements_in_file(elements_in_file) {
      header_pos = ofs.tellp();
      ofs.write(reinterpret_cast<char *>(&elements_in_file),
                sizeof(unsigned long));
    }

    explicit Writer(std::ofstream &ofs) : Writer(ofs, 0) {}

    template <typename T> void write_value(const T &input) { ofs << input; }
    template <typename T> void fix_headers(const T &input) {
      elements_in_file = input;
      auto offset = ofs.tellp();
      ofs.seekp(header_pos, std::ios::beg);
      ofs.write(reinterpret_cast<const char *>(&input), sizeof(unsigned long));
      ofs.seekp(offset);
    }
  };
};
} // namespace ExternalSort
#endif // EXTERNAL_SORT_ULHEADERIOHANDLER_HPP
