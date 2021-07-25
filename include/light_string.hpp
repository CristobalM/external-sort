#ifndef _LIGHT_STRING_HPP_H
#define _LIGHT_STRING_HPP_H

#include <algorithm>
#include <cstring>
#include <ostream>
#include <string>

class light_string {
  char *buf;

public:
  light_string() { buf = nullptr; }
  explicit light_string(char *s) {
    auto size = strlen(s);
    buf = new char[size + 1];
    strcpy(buf, s);
  }
  explicit light_string(const std::string &s) {
    buf = new char[s.size() + 1];
    strcpy(buf, s.c_str());
  }

  ~light_string() {
    if (buf) {
      delete[] buf;
      buf = nullptr;
    }
  }

  light_string(const light_string &other) noexcept {
    auto size = strlen(other.buf);
    buf = new char[size + 1];
    strcpy(buf, other.buf);
  }

  light_string(light_string &&other) noexcept {
    buf = nullptr;
    std::swap(buf, other.buf);
  }

  light_string &operator=(const light_string &other) noexcept {
    auto size = strlen(other.buf);
    buf = new char[size + 1];
    strcpy(buf, other.buf);
    return *this;
  }

  light_string &operator=(light_string &&other) noexcept {
    buf = nullptr;
    std::swap(buf, other.buf);
    return *this;
  }

  bool operator==(const light_string &rhs) const {
    if (!buf)
      return rhs.buf;
    return strcmp(buf, rhs.buf) == 0;
  }

  bool operator!=(const light_string &rhs) const { return !(*this == rhs); }
  bool operator<(const light_string &rhs) const {
    if (!buf)
      return rhs.buf;
    return strcmp(buf, rhs.buf) < 0;
  }
  bool operator>(const light_string &rhs) const { return !(*this < rhs); }
  bool operator<=(const light_string &rhs) const {
    if (!buf)
      return true;
    return strcmp(buf, rhs.buf) <= 0;
  }
  bool operator>=(const light_string &rhs) const {
    if (!buf)
      return rhs.buf;
    return strcmp(buf, rhs.buf) >= 0;
  }

  friend std::ostream &operator<<(std::ostream &output, const light_string &s) {
    if (!s.buf)
      return output;
    output << s.buf;
    return output;
  }

  unsigned long size() const {
    if (!buf)
      return 0;
    return strlen(buf);
  }
};

#endif /* _LIGHT_STRING_HPP_H */