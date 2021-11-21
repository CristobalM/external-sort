//
// Created by cristobal on 7/24/21.
//

#ifndef EXTERNAL_SORT_LIGHTSTRINGSORTCONNECTOR_HPP
#define EXTERNAL_SORT_LIGHTSTRINGSORTCONNECTOR_HPP

#include "light_string.hpp"
namespace ExternalSort {

struct LightStringSortConnector {
  light_string input_string;

public:
  static constexpr bool fixed_size = false;

  explicit LightStringSortConnector(light_string &&input_string)
      : input_string(std::move(input_string)) {}

  LightStringSortConnector() = default;

  LightStringSortConnector(LightStringSortConnector &&other) noexcept
      : input_string(std::move(other.input_string)) {}

  LightStringSortConnector(const LightStringSortConnector &other) = default;

  LightStringSortConnector &
  operator=(LightStringSortConnector &&other) noexcept {
    this->input_string = std::move(other.input_string);
    return *this;
  }

  LightStringSortConnector &
  operator=(const LightStringSortConnector &other) = default;

  size_t size() const { return input_string.size(); }

  struct Comparator {
    bool operator()(const LightStringSortConnector &lhs,
                    const LightStringSortConnector &rhs) {
      return lhs.input_string < rhs.input_string;
    }
  };

  static LightStringSortConnector from_string_line(const std::string &line) {
    return LightStringSortConnector(light_string(line));
  }

  friend std::ostream &operator<<(std::ostream &os,
                                  const LightStringSortConnector &data);

  bool operator==(const LightStringSortConnector &other) const {
    return input_string == other.input_string;
  }

  bool operator!=(const LightStringSortConnector &other) const {
    return input_string != other.input_string;
  }

  bool operator<(const LightStringSortConnector &other) const {
    return input_string < other.input_string;
  }

  static bool read_value(std::ifstream &ifs,
                         LightStringSortConnector &next_val) {
    std::string line;
    auto was_read = (bool)std::getline(ifs, line);
    if (was_read) {
      next_val = LightStringSortConnector(light_string(line));
    }
    return was_read;
  }
};

std::ostream &operator<<(std::ostream &os,
                         const LightStringSortConnector &data) {
  os << data.input_string << "\n";
  return os;
}
} // namespace ExternalSort

#endif // EXTERNAL_SORT_LIGHTSTRINGSORTCONNECTOR_HPP
