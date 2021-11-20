//
// Created by cristobal on 20-11-21.
//

#ifndef EXTERNAL_SORT_DEFAULTREADER_HPP
#define EXTERNAL_SORT_DEFAULTREADER_HPP

#include <istream>

class DefaultReader {
  std::ifstream &is;
public:
  explicit DefaultReader(std::ifstream &is) : is(is){}

  template <typename T>
  bool read_value(T &out){
    return T::read_value(is, out);
  }
};

#endif // EXTERNAL_SORT_DEFAULTREADER_HPP
