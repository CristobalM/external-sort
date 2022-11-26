#ifndef _PARALLEL_WORKER_HPP_
#define _PARALLEL_WORKER_HPP_

#include <condition_variable>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>
#include <cstddef>

class ParallelWorkerQueue {
  std::mutex mutex;
  std::deque<std::function<void()>> q;

public:
  void add_task(std::function<void()> &fun) {
    std::lock_guard<std::mutex> lg(mutex);
    q.push_back(fun);
  }

  bool empty() {
    std::lock_guard<std::mutex> lg(mutex);
    return q.empty();
  }

  std::function<void()> pop() {
    std::lock_guard<std::mutex> lg(mutex);
    auto fun = q.front();
    q.pop_front();
    return fun;
  }
};

class ParallelWorker {
  ParallelWorkerQueue &queue;
  std::mutex &shared_mutex;
  std::condition_variable &queue_cv;

  std::mutex mutex_stop;

  std::unique_ptr<std::thread> th;
  volatile bool _stopped;

  int worker_id;

public:
  ParallelWorker(ParallelWorkerQueue &queue, std::mutex &shared_mutex,
                 std::condition_variable &queue_cv)
      : queue(queue), shared_mutex(shared_mutex), queue_cv(queue_cv),
        _stopped(false) {
    static int workers_count = 0;
    worker_id = workers_count++;
    start();
  }

  void join() { th->join(); }

  void stop() { set_stopped(true); }

private:
  void start() {
    th = std::make_unique<std::thread>(&ParallelWorker::run, this);
  }

  bool stopped() {
    std::lock_guard<std::mutex> lg(mutex_stop);
    return _stopped;
  }

  void set_stopped(bool next_value) {
    std::lock_guard<std::mutex> lg(mutex_stop);
    _stopped = next_value;
  }

  void run() {
    while (!stopped() || !queue.empty()) {
      std::unique_lock<std::mutex> ul(shared_mutex);
      queue_cv.wait(ul, [this]() { return stopped() || !queue.empty(); });
      if (stopped() && queue.empty())
        break;
      if (queue.empty())
        continue;
      auto task = queue.pop();
      ul.unlock();
      queue_cv.notify_all();
      task();
    }
    queue_cv.notify_all();
  }
};

class ParallelWorkerPool {
  ParallelWorkerQueue queue;
  std::vector<std::unique_ptr<ParallelWorker>> workers;

  std::mutex shared_mutex;
  std::condition_variable queue_cv;

public:
  ParallelWorkerPool(int workers_amount) {
    for (int i = 0; i < workers_amount; i++) {
      workers.push_back(
          std::make_unique<ParallelWorker>(queue, shared_mutex, queue_cv));
    }
  }

  void add_task(std::function<void()> &&task) {
    queue.add_task(task);
    queue_cv.notify_all();
  }

  void wait_workers() {
    for (auto &w : workers)
      w->join();
  }

  void stop_all_workers() {
    for (auto &w : workers)
      w->stop();
    queue_cv.notify_all();
  }

  size_t workers_size() const { return workers.size(); }
};

#endif /* _PARALLEL_WORKER_HPP_ */
