//
// Created by cristobal on 7/16/21.
//

#ifndef ESTIMECONTROL_HPP
#define ESTIMECONTROL_HPP

#include <chrono>
#include <memory>
#include <string>

namespace ExternalSort {
class ESTimeControl {
  long ticks_until_check;
  std::chrono::milliseconds time_duration;

  std::chrono::time_point<std::chrono::system_clock> starting_time;
  long current_ticks;
  bool time_has_passed;

public:
  static constexpr bool with_time_control = true;

  bool tick() {
    current_ticks++;
    if (time_has_passed)
      return false;
    if (current_ticks < ticks_until_check)
      return true;
    current_ticks = 0;
    auto current_time = std::chrono::system_clock::now();
    if (current_time - starting_time > time_duration) {
      time_has_passed = true;
      return false;
    }
    return true;
  }
  ESTimeControl(long ticks_until_check, std::chrono::milliseconds time_duration)
      : ticks_until_check(ticks_until_check), time_duration(time_duration),
        current_ticks(0), time_has_passed(false) {}
  void start_timer() { starting_time = std::chrono::system_clock::now(); }
  bool finished() const { return !time_has_passed; }
  void tick_only_count() { current_ticks++; }
};
} // namespace ExternalSort

#endif // ESTIMECONTROL_HPP
