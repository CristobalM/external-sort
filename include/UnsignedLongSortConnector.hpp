//
// Created by cristobal on 7/24/21.
//

#ifndef EXTERNAL_SORT_UNSIGNEDLONGSORTCONNECTOR_HPP
#define EXTERNAL_SORT_UNSIGNEDLONGSORTCONNECTOR_HPP

#include <fstream>

namespace ExternalSort {

class UnsignedLongSortConnector {
  unsigned long value;

public:
  static constexpr bool fixed_size = true;

  explicit UnsignedLongSortConnector(unsigned long value) : value(value) {}

  UnsignedLongSortConnector() : value(0) {}

  UnsignedLongSortConnector(UnsignedLongSortConnector &&other) noexcept
      : value(other.value) {}
  UnsignedLongSortConnector(const UnsignedLongSortConnector &other) = default;

  UnsignedLongSortConnector &
  operator=(UnsignedLongSortConnector &&other) noexcept {
    this->value = other.value;
    return *this;
  }
  UnsignedLongSortConnector &
  operator=(const UnsignedLongSortConnector &other) = default;

  struct Comparator {
    bool operator()(const UnsignedLongSortConnector &lhs,
                    const UnsignedLongSortConnector &rhs) {
      return lhs.value < rhs.value;
    }
  };

  friend std::ostream &operator<<(std::ostream &os,
                                  const UnsignedLongSortConnector &data);

  bool operator==(const UnsignedLongSortConnector &other) const {
    return value == other.value;
  }

  bool operator!=(const UnsignedLongSortConnector &other) const {
    return value != other.value;
  }

  static bool read_value(std::ifstream &ifs,
                         UnsignedLongSortConnector &next_val) {
    unsigned long value;
    ifs.read(reinterpret_cast<char *>(&value), sizeof(unsigned long));

    if ((ifs.rdstate() & std::ifstream::eofbit) != 0) {
      return false;
    }
    next_val = UnsignedLongSortConnector(value);
    return true;
  }

  static size_t size() { return sizeof(unsigned long); }
};
std::ostream &operator<<(std::ostream &os,
                         const UnsignedLongSortConnector &data) {
  os.write(reinterpret_cast<const char *>(&data.value), sizeof(unsigned long));
  return os;
}

} // namespace ExternalSort

#endif // EXTERNAL_SORT_UNSIGNEDLONGSORTCONNECTOR_HPP
