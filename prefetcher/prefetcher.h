#pragma once
#include <assert.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <map>
#include <set>
#include <thread>
#include <unordered_map>
#include <cstring>
#include <list>

#include <hdr/hdr_histogram.h>

#include "db/db_impl/db_impl.h"
#include "db/event_helpers.h"
#include "db/version_edit.h"
#include "logging/event_logger.h"
#include "rocksdb/options.h"
#include "table/block_based/block_based_table_reader.h"
#include "util/mutexlock.h"
#include <unordered_map>


#include "db/db_impl/db_impl.h"

#define NS_PER_SECOND 1000000000  //一秒的纳秒数
#define NS_PER_USECOND 1000       //一微秒的纳秒数
#define BUF_SIZE 1024 * 1024

namespace rocksdb {

struct DLinkedNode {
    std::string key, value;
    DLinkedNode* prev;
    DLinkedNode* next;
    DLinkedNode(): key(""), value(""), prev(nullptr), next(nullptr) {}
    DLinkedNode(std::string _key, std::string _value): key(_key), value(_value), prev(nullptr), next(nullptr) {}
};

class LRUCache {
private:
    std::unordered_map<std::string, DLinkedNode*> cache;
    DLinkedNode* head;
    DLinkedNode* tail;
    int size;
    int capacity;

public:
    LRUCache(): capacity(200*1024), size(0) {
        // 使用伪头部和伪尾部节点
        head = new DLinkedNode();
        tail = new DLinkedNode();
        head->next = tail;
        tail->prev = head;
    }
    LRUCache(int _capacity): capacity(_capacity), size(0) {
        // 使用伪头部和伪尾部节点
        head = new DLinkedNode();
        tail = new DLinkedNode();
        head->next = tail;
        tail->prev = head;
    }
    
    std::string get(std::string key) {
        if (!cache.count(key)) {
            return "notfound42316589/72";
        }
        // 如果 key 存在，先通过哈希表定位，再移到头部
        DLinkedNode* node = cache[key];
        moveToHead(node);
        // fprintf(stderr,"hit in cache\n");
        return node->value;
    }
    
    void put(std::string key, std::string value) {
        // fprintf(stderr,"cache size %d cap %d\n",size,capacity);
        if (!cache.count(key)) {
            // 如果 key 不存在，创建一个新的节点
            DLinkedNode* node = new DLinkedNode(key, value);
            // 添加进哈希表
            cache[key] = node;
            // 添加至双向链表的头部
            addToHead(node);
            ++size;
            if (size > capacity) {
                // 如果超出容量，删除双向链表的尾部节点
                DLinkedNode* removed = removeTail();
                // 删除哈希表中对应的项
                cache.erase(removed->key);
                // 防止内存泄漏
                delete removed;
                --size;
            }
        }
        else {
            // 如果 key 存在，先通过哈希表定位，再修改 value，并移到头部
            DLinkedNode* node = cache[key];
            node->value = value;
            moveToHead(node);
        }
    }

    void addToHead(DLinkedNode* node) {
        node->prev = head;
        node->next = head->next;
        head->next->prev = node;
        head->next = node;
    }
    
    void removeNode(DLinkedNode* node) {
        node->prev->next = node->next;
        node->next->prev = node->prev;
    }

    void moveToHead(DLinkedNode* node) {
        removeNode(node);
        addToHead(node);
    }

    DLinkedNode* removeTail() {
        DLinkedNode* node = tail->prev;
        removeNode(node);
        return node;
    }
};

class SstTemp {
 public:
  uint64_t sst_id_blk;
  double get_times;  //访问次数
  SstTemp(uint64_t sst_id_blk_) {
    sst_id_blk = sst_id_blk_;
    get_times = 1;
  }

  SstTemp(uint64_t sst_id_blk_, double get_times_) {
    sst_id_blk = sst_id_blk_;
    get_times = get_times_;
  }
  ~SstTemp()
  {
  }
};

typedef std::pair<uint64_t, SstTemp*> PAIR;
struct CmpByValue {
  bool operator()(const PAIR& lhs, const PAIR& rhs) {
    if (lhs.second == nullptr && rhs.second == nullptr) {
      return true;
    } else if (lhs.second == nullptr) {
      return true;
    } else if (rhs.second == nullptr) {
      return false;
    }
    return lhs.second->get_times < rhs.second->get_times;
  }
};

class SstManager {
 public:
  std::unordered_map<uint64_t, SstTemp*> sstMap;
  
  std::vector<PAIR> sortedV;
  bool isSorted = false;
  int maxIndex=0;
  int minIndex=0;
  void sortSst() {
    sortedV.clear();
    sortedV.insert(sortedV.begin(), sstMap.begin(), sstMap.end());
    sort(sortedV.begin(), sortedV.end(), CmpByValue());
    maxIndex=sortedV.size()-1;
    minIndex=0;
  }
  void subMaxIndex()
  {
    maxIndex--;
  }
  void addMinIndex()
  {
    minIndex++;
  }
  PAIR getMaxSorted() {
    if(!(maxIndex<sortedV.size()&&maxIndex>=0))
    {
      PAIR t;
      t.second=nullptr;
      return t;
    }
    return sortedV[maxIndex];
  }
  PAIR getMinSorted() {
    if(minIndex>=sortedV.size())
    {
      PAIR t;
      t.second=nullptr;
      return t;
    }
    return sortedV[minIndex];
  }
  uint64_t getMax() {
    uint64_t key = 0;
    double num = 0;
    for (auto it = sstMap.begin(); it != sstMap.end(); it++) {
      if (it->second->get_times > num || num == 0) {
        num = it->second->get_times;
        key = it->first;
      }
    }
    return key;
  }
  uint64_t getMin() {
    uint64_t key = 0;
    double num = UINT32_MAX;
    for (auto it = sstMap.begin(); it != sstMap.end(); it++) {
      if (it->second->get_times < num) {
        num = it->second->get_times;
        key = it->first;
      }
    }
    return key;
  }
};

#define IOPS_MAX 1000

class Prefetcher {
 public:
  int calcuTimes=0;
  BlockBasedTableOptions* options_=nullptr;
  LRUCache * cache=nullptr;
  DBImpl *impl_;
  bool paused=false;
  std::list<int> lastIOPS;//过去10秒内的IOPS
  uint64_t hit_times=0;
  uint64_t all_times=0;

  uint64_t all_prefetch_hit_times=0;
  uint64_t all_prefetch_all_times=0;
  uint64_t all_blkcache_all_times=0;
  uint64_t all_blkcache_hit_times=0;

  uint64_t prefetch_times=0;

  uint64_t blkcache_insert_times=0;

  uint64_t blkcache_all_times=0;
  uint64_t blkcache_hit_times=0;
  uint64_t KVcache_all_times=0;
  uint64_t KVcache_hit_times=0;
  std::unordered_map<uint64_t, char*> sst_blocks;
  std::vector<char*> mems;

  static Prefetcher& _GetInst();
  char* buf_ = nullptr;
  bool inited = false;

  bool logRWlat=false;

  static void Init(DBImpl *impl,bool doPrefetch_);
  void _Init(DBImpl *impl,bool doPrefetch_);
  static void SetOptions(BlockBasedTableOptions* options);
  static int64_t now();

  struct hdr_histogram *hdr_last_1s_read = NULL;
  struct hdr_histogram *hdr_last_1s_write = NULL;
  struct hdr_histogram *hdr_last_1s_size = NULL;
  FILE *logFp_read = nullptr;
  FILE *logFp_write = nullptr;
  FILE *logFp_size = nullptr;
  FILE *logFp_prefetch_times = nullptr;
  FILE *logFp_limiter_time = nullptr;

  FILE *tempLog=nullptr;

  uint64_t limiter_star_time=0;

  int log_time=0;
  uint64_t prefetch_start=0;
  int t_prefetch_times=0;

  int readiops = 0;
  int writeiops = 0;
  uint64_t tiktoks = 0;
  uint64_t tiktok_start = 0;


  static void RecordTime(int op, uint64_t tx_xtime,size_t size);
  void _RecordTime(int op, uint64_t tx_xtime,size_t size);
  void latency_hiccup_read(uint64_t iops,uint64_t alliops);
  void latency_hiccup_write(uint64_t iops);
  void latency_hiccup_size();

  void log_prefetch_times(int time,int times);

  ~Prefetcher();
   

  

  Env* env_ = nullptr;
  mutable port::Mutex lock_;        // synchronization primitive
  mutable port::Mutex lock_sst_io;  // synchronization primitive


  mutable port::Mutex lock_rw;        // synchronization primitive

  mutable port::Mutex lock_mem;        // synchronization primitive



  std::unordered_map<uint64_t, int>
      sst_iotimes;  //统计一秒内的sst块(256k)的io次数 key : sstidk
                    //(后4位k为第几个256k块 从0开始)

  SstManager cloudManager;
  SstManager ssdManager;

  static void SstRead(uint64_t sst_id, uint64_t offset, size_t size,
                      bool isGp2);  //更新sst的读写次数(热度)
  void _SstRead(uint64_t sst_id, uint64_t offset, size_t size, bool isGp2);
  static void CaluateSstHeat();
  void _CaluateSstHeat();
  static void Prefetche();
  void _Prefetcher();

  void _PrefetcherToMem();
  void _PrefetcherToMem2();

  static size_t TryGetFromPrefetcher(uint64_t sst_id, uint64_t offset, size_t n,
                                     char* scratch);
  size_t _TryGetFromPrefetcher(uint64_t sst_id, uint64_t offset, size_t n,
                               char* scratch);

  size_t _PrefetcherOneFile(uint64_t key, uint64_t offset, size_t n,
                            char* scratch);
  size_t _PrefetcherTwoFiles(uint64_t key, uint64_t offset, size_t n,
                             char* scratch);


  size_t _PrefetcherFromMem(uint64_t key, uint64_t offset, size_t n,
                            char* scratch);

    size_t _Prefetcher2BlocksFromMem(uint64_t key, uint64_t offset, size_t n,
                            char* scratch);

  static void blkcacheInsert();
  static void blkcacheTryGet();
  static void blkcacheGet();

  static void KVcacheTryGet();
  static void KVcacheGet();

  static std::string CacheGet(std::string key);
  static void CachePut(std::string key,std::string value);

  static void RecordLimiterTime(uint64_t prefetch,uint64_t compaction,uint64_t flush);

  static bool getCompactionPaused();
};
}  // namespace rocksdb