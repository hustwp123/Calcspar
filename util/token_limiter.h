#pragma once

#include <cstdint>
#include <deque>
#include <memory>
#include <queue>
#include <random>
#include <ratio>

#include "port/port.h"
#include "port/port_posix.h"
#include "rocksdb/env.h"

namespace rocksdb {

class TokenLimiter {
 public:
  int subtimes=0;
  int addtimes=5;
  int I_middle=0; //middle IOPS
  int I_low=0;
  uint64_t T_middle=0; //middle 时间片
  uint64_t T_low=0;
  uint64_t refill_us=0;
  bool updated=false;
  int R_1=850; //前一秒到middle时间片时所剩余令牌数
  int R_2=-1000; //前2秒到middle时间片时所剩余令牌数

  enum IOType {
    kRead,
    kWrite,
  };

 private:
  class Req {
   public:
    port::CondVar cv_;
    int32_t n_;
    bool granted_;
    explicit Req(port::Mutex* mu, int32_t n)
        : cv_(mu), n_(n), granted_(false) {}
  };

 private:
  Env* const env_ = Env::Default();
  port::Mutex request_mutex_;
  uint32_t tokens_per_sec_;

  int32_t available_tokens_;
  uint64_t next_refill_sec_;
  // 900 * 1000, 700 * 1000, 500 * 1000, 0

  const uint64_t limits[Env::IOSource::IO_SRC_DEFAULT] = {
      900 * 1000, 800 * 1000, 500 * 1000, 0};
  //
  // Prefetch, Compaction, Flush, User
  uint64_t wait_threshold_us_[Env::IOSource::IO_SRC_DEFAULT];
  uint64_t total_requests_[IOType::kWrite + 1]
                          [Env::IOSource::IO_SRC_DEFAULT + 1];

  // IO_SRC_DEFAULT will not enqueue, just bypass.
  std::deque<Req*> queues_[Env::IOSource::IO_SRC_DEFAULT];
  std::mt19937 rng_;
  int32_t requests_to_wait_;
  port::CondVar exit_cv_;
  bool has_pending_waiter_;
  bool stop_;

 public:
  static TokenLimiter* GetDefaultInstance();
  static void SetDefaultInstance(std::unique_ptr<TokenLimiter> limiter);
  static void RequestDefaultToken(Env::IOSource io_src, IOType io_type,
                                  int32_t n = 1);
  static void PrintStatus();
  static bool ManualTune(Env::IOSource io_src, uint64_t wait_threshold_us);

  static void TunePriority(Env::IOSource io_src, bool add);

 public:
  explicit TokenLimiter(int32_t tokens_per_sec);
  ~TokenLimiter();
  void RequestToken(Env::IOSource io_src, IOType io_type, int32_t n = 1);
  static std::string IOSourceToString(Env::IOSource io_src);

 private:
  // lock must hold before calling this function.
  bool RefillIfNeeded(uint64_t now_sec);
  // calc wait time point.
  uint64_t CalcWakeMicros();
  // dispatch token to req in queue
  void DispatchToken(uint64_t now_us);
  bool Tune(Env::IOSource io_src, uint64_t wait_threshold_us);
  void TunePriority_(Env::IOSource io_src, bool add);
};

}  // namespace rocksdb
