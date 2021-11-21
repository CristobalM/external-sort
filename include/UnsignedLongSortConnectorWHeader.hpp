//
// Created by cristobal on 21-11-21.
//

#ifndef EXTERNAL_SORT_UNSIGNEDLONGSORTCONNECTORWHEADER_HPP
#define EXTERNAL_SORT_UNSIGNEDLONGSORTCONNECTORWHEADER_HPP

#include <fstream>

namespace ExternalSort {

class UnsignedLongSortConnectorWHeader {
  unsigned long value;

public:
  static constexpr bool fixed_size = true;

  explicit UnsignedLongSortConnectorWHeader(unsigned long value)
      : value(value) {}

  UnsignedLongSortConnectorWHeader() : value(0) {}

  UnsignedLongSortConnectorWHeader(
      UnsignedLongSortConnectorWHeader &&other) noexcept
      : value(other.value) {}
  UnsignedLongSortConnectorWHeader(
      const UnsignedLongSortConnectorWHeader &other) = default;

  UnsignedLongSortConnectorWHeader &
  operator=(UnsignedLongSortConnectorWHeader &&other) noexcept {
    this->value = other.value;
    return *this;
  }
  UnsignedLongSortConnectorWHeader &
  operator=(const UnsignedLongSortConnectorWHeader &other) = default;

  struct Comparator {
    bool operator()(const UnsignedLongSortConnectorWHeader &lhs,
                    const UnsignedLongSortConnectorWHeader &rhs) {
      return lhs.value < rhs.value;
    }
  };

  friend std::ostream &operator<<(std::ostream &os,
                                  const UnsignedLongSortConnectorWHeader &data);

  bool operator==(const UnsignedLongSortConnectorWHeader &other) const {
    return value == other.value;
  }

  bool operator!=(const UnsignedLongSortConnectorWHeader &other) const {
    return value != other.value;
  }

  static bool read_value(std::ifstream &ifs,
                         UnsignedLongSortConnectorWHeader &next_val) {
    unsigned long value;
    ifs.read(reinterpret_cast<char *>(&value), sizeof(unsigned long));

    if ((ifs.rdstate() & std::ifstream::eofbit) != 0) {
      return false;
    }
    next_val = UnsignedLongSortConnectorWHeader(value);
    return true;
  }

  static size_t size() { return sizeof(unsigned long); }
};

std::ostream &operator<<(std::ostream &os,
                         const UnsignedLongSortConnectorWHeader &data) {
  os.write(reinterpret_cast<const char *>(&data.value), sizeof(unsigned long));
  return os;
}

} // namespace ExternalSort
#endif // EXTERNAL_SORT_UNSIGNEDLONGSORTCONNECTORWHEADER_HPP
