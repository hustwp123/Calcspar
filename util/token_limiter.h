#pragma once

#include <cstdint>
#include <deque>
#include <memory>
#include <queue>
#include <ratio>

#include "port/port.h"
#include "port/port_posix.h"
#include "rocksdb/env.h"

namespace rocksdb {

class TokenLimiter {
 public:
  enum IOType {
    kRead,
    kWrite,
  };

 private:
  class Req {
   public:
    port::CondVar cv_;
    uint64_t enter_ns_;
    bool granted_;
    explicit Req(port::Mutex* mu, uint64_t enter_ns)
        : cv_(mu), enter_ns_(enter_ns), granted_(false) {}
  };

 private:
  Env* const env_ = Env::Default();
  port::Mutex request_mutex_;
  uint32_t tokens_per_sec_;

  int32_t available_tokens_;
  uint64_t next_refill_sec_;
  // 900 * 1000000, 700 * 1000000, 500 * 1000000, 0
  //
  // Prefetch, Compaction, Flush, User
  uint64_t wait_threshold_ns_[Env::IOSource::IO_SRC_DEFAULT];
  uint64_t total_requests_[IOType::kWrite + 1]
                          [Env::IOSource::IO_SRC_DEFAULT + 1];

  // IO_SRC_DEFAULT will not enqueue, just bypass.
  std::deque<Req*> queues_[Env::IOSource::IO_SRC_DEFAULT];

  int32_t requests_to_wait_;
  port::CondVar exit_cv_;
  bool has_pending_waiter_;
  bool stop_;

 public:
  static TokenLimiter* GetDefaultInstance();
  static void SetDefaultInstance(std::unique_ptr<TokenLimiter> limiter);
  static void RequestDefaultToken(Env::IOSource io_src, IOType io_type);
  static void PrintStatus();

 public:
  explicit TokenLimiter(int32_t tokens_per_sec);
  ~TokenLimiter();
  void RequestToken(Env::IOSource io_src, IOType io_type);

 private:
  // lock must hold before calling this function.
  bool RefillIfNeeded(uint64_t now_sec);
  // calc wait time point.
  uint64_t CalcWakeMicros();
  // dispatch token to req in queue
  void DispatchToken(uint64_t now_ns);
  void TunePrefetch();

 private:
  static uint64_t NowSec(Env* env) { return env->NowNanos() / std::nano::den; }
  static uint64_t NowSecInNanos(Env* env) {
    return TokenLimiter::NowSec(env) * std::nano::den;
  }
  static uint64_t NowMicros(Env* env) {
    return env->NowNanos() / std::micro::den;
  }
  static std::string IOSourceToString(Env::IOSource io_src);
};

}  // namespace rocksdb
