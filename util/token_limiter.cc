#include "token_limiter.h"

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <iostream>
#include <memory>
#include <ratio>

#include "util/mutexlock.h"

namespace rocksdb {

static std::unique_ptr<TokenLimiter> default_limiter = nullptr;

// static----------------------------------------------------------------------

TokenLimiter* TokenLimiter::GetDefaultInstance() {
  return default_limiter.get();
}

void TokenLimiter::SetDefaultInstance(std::unique_ptr<TokenLimiter> limiter) {
  if(default_limiter != nullptr)
  {
    return;
  }
  // fprintf(stderr,"default_limiter==null %d limiter==null %d\n",default_limiter == nullptr,limiter == nullptr);
  assert(default_limiter == nullptr && limiter != nullptr);
  default_limiter = std::move(limiter);
}

void TokenLimiter::RequestDefaultToken(Env::IOSource io_src, IOType io_type) {
  if (default_limiter != nullptr) {
    default_limiter->RequestToken(io_src, io_type);
  }
}

void TokenLimiter::PrintStatus() {
  if (default_limiter != nullptr) {
    for (int i = Env::IO_SRC_PREFETCH; i <= Env::IO_SRC_DEFAULT; i++) {
      std::cout << "IOSource: "
                << TokenLimiter::IOSourceToString((Env::IOSource)i)
                << std::endl;
      for (auto j = 0; j <= IOType::kWrite; j++) {
        std::cout << "  " << (j == 0 ? "Read" : "Write") << ": "
                  << default_limiter->total_requests_[j][i] << std::endl;
      }
    }
  }
}

std::string TokenLimiter::IOSourceToString(Env::IOSource io_src) {
  switch (io_src) {
    case Env::IOSource::IO_SRC_PREFETCH:
      return "Prefetch";
    case Env::IOSource::IO_SRC_COMPACTION:
      return "Compaction";
    case Env::IOSource::IO_SRC_FLUSH_L0COMP:
      return "FLUSH_L0COMP";
    case Env::IOSource::IO_SRC_USER:
      return "User";
    case Env::IOSource::IO_SRC_DEFAULT:
      return "Default";
    default:
      return "Unknown";
  }
}

// ----------------------------------------------------------------------------

TokenLimiter::TokenLimiter(int32_t token_per_sec)
    : tokens_per_sec_(token_per_sec),
      available_tokens_(token_per_sec),
      next_refill_sec_(env_->NowMicros() / std::micro::den + 1),
      wait_threshold_us_{900 * 1000, 700 * 1000, 0, 0},
      total_requests_{{0, 0, 0, 0, 0}, {0, 0, 0, 0, 0}},

      queues_{std::deque<Req*>(), std::deque<Req*>(), std::deque<Req*>(),
              std::deque<Req*>()},
      requests_to_wait_(0),
      exit_cv_(&request_mutex_),
      has_pending_waiter_(false),
      stop_(false) {
  assert(tokens_per_sec_ > 0);
}

TokenLimiter::~TokenLimiter() {
  // TODO: resolve all waiters
  MutexLock g(&request_mutex_);
  stop_ = true;
  uint64_t waiting_reqs = 0;
  for (int i = Env::IO_SRC_PREFETCH; i <= Env::IO_SRC_USER; i++) {
    waiting_reqs += queues_[i].size();
  }
  requests_to_wait_ = waiting_reqs;
  for (int i = Env::IO_SRC_USER; i >= Env::IO_SRC_PREFETCH; i--) {
    for (auto& r : queues_[i]) {
      r->cv_.Signal();
    }
  }
  while (requests_to_wait_ > 0) {
    exit_cv_.Wait();
  }
}

void TokenLimiter::RequestToken(Env::IOSource io_src, IOType io_type) {
  assert(io_src >= Env::IOSource::IO_SRC_PREFETCH &&
         io_src <= Env::IOSource::IO_SRC_DEFAULT);
  assert(io_type >= TokenLimiter::IOType::kRead &&
         io_type <= TokenLimiter::IOType::kWrite);

  MutexLock g(&request_mutex_);
  total_requests_[io_type][io_src]++;

  if (stop_ || io_src == Env::IOSource::IO_SRC_DEFAULT) {
    return;
  }

  uint64_t arrive_us = env_->NowMicros();
  uint64_t arrive_sec = arrive_us / std::micro::den;
  uint64_t arrive_sec_in_us = arrive_sec * std::micro::den;
  // we can not take token in previous second, so we need to check for refill
  RefillIfNeeded(arrive_sec);
  // ?should we design to signal to the waiter here

  if (available_tokens_ > 0 &&
      arrive_us >= arrive_sec_in_us + wait_threshold_us_[io_src] &&
      queues_[io_src].empty()) {
    available_tokens_--;
    return;
  }

  Req req(&request_mutex_);
  queues_[io_src].push_back(&req);

  do {
    if (has_pending_waiter_) {
      // there is a waiter, wait for wake up
      req.cv_.Wait();
      // after wake up, there are three conditions
      // (1) db exit, stop_ is true, will not enter this loop again
      // (2) grant a token, will not enter this loop again
      // (3) previous waiter wake me up to wait for next token fill
    } else {
      // we are the waiter to wait for next time
      has_pending_waiter_ = true;
      // here will release the lock
      req.cv_.TimedWait(CalcWakeMicros());
      // grant the lock again, it is safe to set to false
      has_pending_waiter_ = false;
      uint64_t wake_up_us = env_->NowMicros();
      RefillIfNeeded(wake_up_us / std::micro::den);
      DispatchToken(wake_up_us);
      // (1) we have token and exit queue now, since we are the waiter,
      //     we should wake up one waiter for next tick
      // (2) we don't have token, just enter next loop to wait
      if (req.granted_) {
        for (int i = Env::IO_SRC_USER; i >= Env::IO_SRC_PREFETCH; i--) {
          if (!queues_[i].empty()) {
            queues_[i].front()->cv_.Signal();
            break;
          }
        }
      }
    }
  } while (!stop_ && !req.granted_);

  // here we exit the queue
  if (stop_) {
    requests_to_wait_--;
    exit_cv_.Signal();
  }
}

bool TokenLimiter::RefillIfNeeded(uint64_t now_sec) {
  if (now_sec >= next_refill_sec_) {
    TunePrefetch();
    // available_tokens_ >= 0, so fill make it tokens_per_sec_
    available_tokens_ = tokens_per_sec_;
    next_refill_sec_ = now_sec + 1;
    return true;
  }
  return false;
}

uint64_t TokenLimiter::CalcWakeMicros() {
  uint64_t now_us = env_->NowMicros();
  uint64_t now_sec_in_us = (now_us / std::micro::den) * std::micro::den;
  if (now_sec_in_us >= next_refill_sec_ * std::micro::den) {
    return next_refill_sec_ * std::micro::den;
  }
  if (available_tokens_ == 0) {
    // no token in this second, just wait util refill
    return next_refill_sec_ * std::micro::den;
  }

  for (int i = Env::IO_SRC_USER; i >= Env::IO_SRC_PREFETCH; i--) {
    if (now_us < now_sec_in_us + wait_threshold_us_[i]) {
      return now_sec_in_us + wait_threshold_us_[i];
    }
  }
  return next_refill_sec_ * std::micro::den;
}

void TokenLimiter::DispatchToken(uint64_t now_us) {
  uint64_t now_sec_in_us = (now_us / std::micro::den) * std::micro::den;
  for (int i = Env::IO_SRC_USER;
       i >= Env::IO_SRC_PREFETCH && available_tokens_ > 0; i--) {
    if (now_us < now_sec_in_us + wait_threshold_us_[i]) {
      break;
    }
    while (!queues_[i].empty() && available_tokens_ > 0) {
      Req* next_req = queues_[i].front();
      available_tokens_--;
      next_req->granted_ = true;
      queues_[i].pop_front();
      next_req->cv_.Signal();
    }
  }
}

void TokenLimiter::TunePrefetch() {
  uint64_t prev = wait_threshold_us_[Env::IO_SRC_PREFETCH];
  if (available_tokens_ > 0) {
    wait_threshold_us_[Env::IO_SRC_PREFETCH] = std::max(
        prev - 50 * 1000, wait_threshold_us_[Env::IO_SRC_PREFETCH + 1]);
  } else {
    wait_threshold_us_[Env::IO_SRC_PREFETCH] =
        std::min(prev + 50 * 1000, (uint64_t)900 * 1000);
  }
}

}  // namespace rocksdb