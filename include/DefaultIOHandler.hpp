//
// Created by cristobal on 20-11-21.
//

#ifndef EXTERNAL_SORT_DEFAULTIOHANDLER_HPP
#define EXTERNAL_SORT_DEFAULTIOHANDLER_HPP

#include <fstream>

namespace ExternalSort {
class DefaultIOHandler {
public:
  class Reader {
    std::ifstream &is;

  public:
    explicit Reader(std::ifstream &is) : is(is) {}

    template <typename T> bool read_value(T &out) {
      return T::read_value(is, out);
    }
  };

  class Writer {
    std::ofstream &ofs;

  public:
    explicit Writer(std::ofstream &ofs, unsigned long) : ofs(ofs) {}

    template <typename T> void write_value(const T &input) {
      // T::write_value(ofs, input);
      ofs << input;
    }

    template <typename T> void fix_headers(const T &) {}
  };
};

} // namespace ExternalSort

#endif // EXTERNAL_SORT_DEFAULTIOHANDLER_HPP
