#include "prefetcher/prefetcher.h"

#include <atomic>

#include "db/db_impl/db_impl.h"
#include "util/token_limiter.h"
#include "zyh/monitor.h"

namespace rocksdb {



const int blkSize = 256 * 1024;
const uint64_t CacheSize=500*1024*1024;
const size_t MAXSSTNUM =  (CacheSize)/(blkSize);  // ssd中缓存的sst_blk的最大数目

std::atomic<bool> pauseComapaction;

std::vector<DbPath> db_paths = {
    {"/home/ubuntu/gp2_150g_1", 60l * 1024 * 1024 * 1024},
    {"/home/ubuntu/ssd_150g", 60l * 1024 * 1024 * 1024},
};

// std::vector<DbPath> db_paths = {
//     {"/home/ubuntu/ssd_150g", 60l * 1024 * 1024 * 1024},
//     {"/home/ubuntu/gp2_150g_1", 60l * 1024 * 1024 * 1024},
// };

Prefetcher::~Prefetcher() {
  pauseComapaction = false;
  fprintf(stderr, "prefetcher hit times: %lu    all times: %lu\n", all_prefetch_hit_times,
          all_prefetch_all_times);
  fprintf(stderr, "blkcache hit times: %lu    all times: %lu\n", all_blkcache_hit_times,all_blkcache_all_times
          );
  fprintf(stderr,
          "prefetcher insert times: %lu    blkcache insert times: %lu\n",
          prefetch_times, blkcache_insert_times);
  if (logFp_read != nullptr) {
    fprintf(logFp_size, "prefetcher hit times: %lu    all times: %lu\n",
            hit_times, all_times);
    fprintf(logFp_size,
            "prefetcher insert times: %lu    blkcache insert times: %lu\n",
            prefetch_times, blkcache_insert_times);
    fclose(logFp_read);
    fclose(logFp_write);
    fclose(logFp_size);
    fclose(logFp_prefetch_times);
    fclose(logFp_limiter_time);

    free(hdr_last_1s_read);
    free(hdr_last_1s_write);
    free(hdr_last_1s_size);
  }

  if(tempLog)
  {
    fprintf(tempLog,"/n/n/n cache hit times\n");
    fprintf(tempLog, "prefetcher hit times: %lu    all times: %lu\n", all_prefetch_hit_times,
          all_prefetch_all_times);
  fprintf(tempLog, "blkcache hit times: %lu    all times: %lu\n", all_blkcache_hit_times,all_blkcache_all_times
          );
    fclose(tempLog);
  }

  if (buf_ != nullptr) {
    free(buf_);
    buf_ = nullptr;
  }
  fprintf(stderr, "~Prefetcher\n");
}

bool doPrefetch = true;
void caluateSstHeatThread()  //统计sst热度线程
{
  while (1) {
    sleep(1);
    Prefetcher::CaluateSstHeat();
  }
}

void prefetchThread()  //统计sst热度线程
{
  while (1) {
    // sleep(1);
    Prefetcher::Prefetche();
  }
}

void Prefetcher::Prefetche() {
  static Prefetcher &i = _GetInst();

  // i._Prefetcher();
  i._PrefetcherToMem();
}
void Prefetcher::RecordLimiterTime(uint64_t prefetch, uint64_t compaction,
                                   uint64_t flush) {
  static Prefetcher &i = _GetInst();
  uint64_t t = now();
  int time = (t - i.limiter_star_time) / 1000000000;
  if (i.logRWlat) {
    fprintf(i.logFp_limiter_time, "%d  %lu  %lu  %lu\n", time, prefetch,
            compaction, flush);
    fflush(i.logFp_limiter_time);
  }
}
void Prefetcher::blkcacheInsert() {
  static Prefetcher &i = _GetInst();
  i.blkcache_insert_times++;
}

void Prefetcher::blkcacheTryGet() {
  static Prefetcher &i = _GetInst();
  i.blkcache_all_times++;
  i.all_blkcache_all_times++;
}
void Prefetcher::blkcacheGet() {
  static Prefetcher &i = _GetInst();
  i.blkcache_hit_times++;
  i.all_blkcache_hit_times++;
}

void Prefetcher::KVcacheTryGet() {
  static Prefetcher &i = _GetInst();
  i.KVcache_all_times++;
}
void Prefetcher::KVcacheGet() {
  static Prefetcher &i = _GetInst();
  i.KVcache_hit_times++;
}

std::string Prefetcher::CacheGet(std::string key) {
  static Prefetcher &i = _GetInst();
  return i.cache->get(key);
}
void Prefetcher::CachePut(std::string key,std::string value){
  static Prefetcher &i = _GetInst();
  i.cache->put(key,value);
}

void Prefetcher::CaluateSstHeat() {
  static Prefetcher &i = _GetInst();
  i._CaluateSstHeat();
}

void Prefetcher::_PrefetcherToMem() {
  lock_.Lock();
  uint64_t start_time=now();
  // fprintf(stderr, "prefetcher begin\n");
  uint64_t fromKey = 0;
  uint64_t outKey = 0;
  if (cloudManager.sstMap.empty())  // cloud中无sst
  {
    lock_.Unlock();
    return;
  }
  fromKey = cloudManager.getMax();
  if(cloudManager.sstMap[fromKey]->get_times<=1.01)
  {
    lock_.Unlock();
    return;
  }
  // fprintf(stderr,"get_times=%lf\n",cloudManager.sstMap[fromKey]->get_times);

  
  // fprintf(stderr,"ssdManager num %d\n",ssdManager.sstMap.size());
  if (ssdManager.sstMap.size() < MAXSSTNUM)  // ssd中未放满
  {
    lock_.Unlock();
  } else  // ssd中已放满
          // 需比较ssd中最冷的sst_blk的热度和cloud中最热的sst_blk的热度 进行替换
  {
    outKey = ssdManager.getMin();
    if ((ssdManager.sstMap[outKey]->get_times) <
        (cloudManager.sstMap[fromKey]->get_times)) {
      lock_.Unlock();
    } else {
      lock_.Unlock();
      return;
    }
  }
  uint64_t fromSstId = fromKey / 100000;
  uint64_t blk_num = fromKey % 100000;
  if (fromSstId == 0) {
    // fprintf(stderr, "err sstid==0 key==%lu gettimes= %u\n", fromKey,
    //         cloudManager.sstMap[fromKey]->get_times);
    lock_.Lock();
    SstTemp *t = cloudManager.sstMap[fromKey];
    cloudManager.sstMap.erase(fromKey);
    delete t;
    lock_.Unlock();
    return;
  }
  std::string old_fname = TableFileName(db_paths, fromSstId, 0);
  int readfd = open(old_fname.c_str(), O_RDONLY | O_DIRECT);
  if (readfd == -1) {
    lock_.Lock();
    fprintf(stderr, "open error fname:%s \n",old_fname.c_str());
    SstTemp *t = cloudManager.sstMap[fromKey];
    cloudManager.sstMap.erase(fromKey);
    delete t;
    lock_.Unlock();
    return;
  }
  uint64_t offset = blk_num * blkSize;
  TokenLimiter::RequestDefaultToken(Env::IOSource::IO_SRC_PREFETCH,
                                    TokenLimiter::kRead);
  int ret = pread(readfd, buf_, blkSize, offset);
  Monitor::CollectIO(6, 1);
  if (ret == -1) {
    fprintf(stderr, "pread error\n");
    close(readfd);
    return;
  }
  close(readfd);
  lock_.Lock();  //更新cloudManager ssdManager
  SstTemp *t = cloudManager.sstMap[fromKey];

  char *temp = nullptr;
  if (!mems.empty()) {
    temp = mems[mems.size() - 1];
    mems.pop_back();
  } else {
    temp = new char[blkSize];
  }
  memcpy(temp, buf_, ret);
  lock_mem.Lock();
  sst_blocks[fromKey] = temp;
  lock_mem.Unlock();

  cloudManager.sstMap.erase(fromKey);
  ssdManager.sstMap[fromKey] = t;
  if (outKey != 0) {
    t = ssdManager.sstMap[outKey];
    ssdManager.sstMap.erase(outKey);
    cloudManager.sstMap[outKey] = t;

    temp = sst_blocks[outKey];
    lock_mem.Lock();
    if (temp != nullptr) {
      sst_blocks.erase(outKey);
      mems.push_back(temp);
    }
    lock_mem.Unlock();
  }
  // fprintf(stderr, "prefetcher end\n");
  // fprintf(stderr,"size=%d use_time=%d\n",ssdManager.sstMap.size(),now()-start_time);

  prefetch_times++;
  t_prefetch_times++;
  uint64_t t_tiktoks = now() - prefetch_start;
  if (t_tiktoks >= 1000000000 && logRWlat) {
    log_prefetch_times(t_tiktoks / 1000000000, t_prefetch_times);
    prefetch_start = now();
    t_prefetch_times = 0;
  }

  lock_.Unlock();
}

void Prefetcher::_PrefetcherToMem2() {
  lock_.Lock();
  uint64_t fromKey = 0;
  uint64_t outKey = 0;
  if (cloudManager.sstMap.empty())  // cloud中无sst
  {
    lock_.Unlock();
    return;
  }
  PAIR from = cloudManager.getMaxSorted();
  if (from.second == nullptr) {
    lock_.Unlock();
    return;
  }
  fromKey = from.first;
  if (ssdManager.sstMap.size() < MAXSSTNUM)  // ssd中未放满
  {
    cloudManager.subMaxIndex();
    lock_.Unlock();
  } else  // ssd中已放满
          // 需比较ssd中最冷的sst_blk的热度和cloud中最热的sst_blk的热度 进行替换
  {
    PAIR out = ssdManager.getMinSorted();
    if (out.second == nullptr) {
      lock_.Unlock();
      return;
    }
    outKey = out.first;
    if (from.second->get_times < out.second->get_times) {
      cloudManager.subMaxIndex();
      ssdManager.addMinIndex();
      lock_.Unlock();
    } else {
      lock_.Unlock();
      return;
    }
    // if ((ssdManager.sstMap[outKey]->get_times) <
    //     (cloudManager.sstMap[fromKey]->get_times)) {
    //   lock_.Unlock();
    // } else {
    //   lock_.Unlock();
    //   return;
    // }
  }
  uint64_t fromSstId = fromKey / 100000;
  uint64_t blk_num = fromKey % 100000;
  if (fromSstId == 0) {
    // fprintf(stderr, "err sstid==0 key==%lu gettimes= %u\n", fromKey,
    //         cloudManager.sstMap[fromKey]->get_times);
    lock_.Lock();
    SstTemp *t = cloudManager.sstMap[fromSstId];
    cloudManager.sstMap.erase(fromSstId);
    delete t;
    lock_.Unlock();
    return;
  }
  std::string old_fname = TableFileName(db_paths, fromSstId, 0);
  int readfd = open(old_fname.c_str(), O_RDONLY | O_DIRECT);
  if (readfd == -1) {
    lock_.Lock();

    SstTemp *t = cloudManager.sstMap[fromSstId];
    cloudManager.sstMap.erase(fromSstId);
    delete t;
    lock_.Unlock();
    return;
  }
  uint64_t offset = blk_num * blkSize;
  TokenLimiter::RequestDefaultToken(Env::IOSource::IO_SRC_PREFETCH,
                                    TokenLimiter::kRead);
  int ret = pread(readfd, buf_, blkSize, offset);
  Monitor::CollectIO(6, 1);
  if (ret == -1) {
    fprintf(stderr, "pread error\n");
    close(readfd);
    return;
  }
  close(readfd);
  lock_.Lock();  //更新cloudManager ssdManager
  SstTemp *t = cloudManager.sstMap[fromKey];
  char *temp = nullptr;
  if (!mems.empty()) {
    temp = mems[mems.size() - 1];
    mems.pop_back();
  } else {
    temp = new char[blkSize];
  }
  memcpy(temp, buf_, ret);

  lock_mem.Lock();
  sst_blocks[fromKey] = temp;
  lock_mem.Unlock();

  cloudManager.sstMap.erase(fromKey);
  ssdManager.sstMap[fromKey] = t;
  if (outKey != 0) {
    t = ssdManager.sstMap[outKey];
    ssdManager.sstMap.erase(outKey);
    cloudManager.sstMap[outKey] = t;

    temp = sst_blocks[outKey];
    lock_mem.Lock();
    if (temp != nullptr) {
      sst_blocks.erase(outKey);
      mems.push_back(temp);
    }
    lock_mem.Unlock();
  }
  // fprintf(stderr, "prefetcher end\n");
  prefetch_times++;
  t_prefetch_times++;
  uint64_t t_tiktoks = now() - prefetch_start;
  if (t_tiktoks >= 1000000000 && logRWlat) {
    log_prefetch_times(t_tiktoks / 1000000000, t_prefetch_times);
    prefetch_start = now();
    t_prefetch_times = 0;
  }

  lock_.Unlock();
}
void Prefetcher::_Prefetcher() {
  lock_.Lock();
  // fprintf(stderr, "prefetcher begin\n");
  uint64_t fromKey = 0;
  uint64_t outKey = 0;
  if (cloudManager.sstMap.empty())  // cloud中无sst
  {
    lock_.Unlock();
    return;
  }
  fromKey = cloudManager.getMax();
  if (ssdManager.sstMap.size() < MAXSSTNUM)  // ssd中未放满
  {
    lock_.Unlock();
  } else  // ssd中已放满
          // 需比较ssd中最冷的sst_blk的热度和cloud中最热的sst_blk的热度 进行替换
  {
    outKey = ssdManager.getMin();
    if ((ssdManager.sstMap[outKey]->get_times) <
        (cloudManager.sstMap[fromKey]->get_times)) {
      lock_.Unlock();
    } else {
      lock_.Unlock();
      return;
    }
  }
  uint64_t fromSstId = fromKey / 100000;
  uint64_t blk_num = fromKey % 100000;
  if (fromSstId == 0) {
    // fprintf(stderr, "err sstid==0 key==%lu gettimes= %u\n", fromKey,
    //         cloudManager.sstMap[fromKey]->get_times);
    lock_.Lock();
    // fprintf(stderr, "file open error1 sstid=%lu\n", fromSstId);
    SstTemp *t = cloudManager.sstMap[fromSstId];
    cloudManager.sstMap.erase(fromSstId);
    delete t;
    lock_.Unlock();
    return;
  }
  std::string old_fname = TableFileName(db_paths, fromSstId, 0);
  std::string new_fname = TableFileName(db_paths, fromKey, 1);
  int readfd = open(old_fname.c_str(), O_RDONLY | O_DIRECT);
  if (readfd == -1) {
    lock_.Lock();
    // fprintf(stderr, "file open error1 sstid=%lu\n", fromSstId);
    SstTemp *t = cloudManager.sstMap[fromSstId];
    cloudManager.sstMap.erase(fromSstId);
    delete t;
    lock_.Unlock();
    return;
  }
  int writefd = open(new_fname.c_str(), O_WRONLY | O_CREAT | O_DIRECT, S_IRWXU);
  if (writefd == -1) {
    fprintf(stderr, "file open error2 sstid=%lu\n", fromSstId);
    close(readfd);
    return;
  }

  uint64_t offset = blk_num * blkSize;
  TokenLimiter::RequestDefaultToken(Env::IOSource::IO_SRC_PREFETCH,
                                    TokenLimiter::kRead);
  int ret = pread(readfd, buf_, blkSize, offset);
  Monitor::CollectIO(6, 1);
  if (ret == -1) {
    fprintf(stderr, "pread error\n");
    close(readfd);
    close(writefd);
    return;
  }
  ret = pwrite(writefd, buf_, blkSize, 0);
  // std::flush(writefd);
  if (ret != blkSize) {
    fprintf(stderr, "pwrite error\n");
    close(readfd);
    close(writefd);
    return;
  }
  close(readfd);
  close(writefd);
  // fprintf(stderr, "prefetcher %lu file\n", fromKey);
  lock_.Lock();  //更新cloudManager ssdManager
  SstTemp *t = cloudManager.sstMap[fromKey];
  cloudManager.sstMap.erase(fromKey);
  ssdManager.sstMap[fromKey] = t;
  if (outKey != 0) {
    t = ssdManager.sstMap[outKey];
    ssdManager.sstMap.erase(outKey);
    cloudManager.sstMap[outKey] = t;
    // fprintf(stderr, "remove %lu file\n", outKey);
    std::string remove_fname = TableFileName(db_paths, outKey, 1);
    if (remove(remove_fname.c_str()) != 0) {
      fprintf(stderr, "remove error\n");
    }
  }
  // fprintf(stderr, "prefetcher end\n");
  lock_.Unlock();
}
void Prefetcher::_CaluateSstHeat() {
  lock_sst_io.Lock();
  std::unordered_map<uint64_t, int> temp_sst_iotimes(sst_iotimes);
  sst_iotimes.clear();
  lock_sst_io.Unlock();
  lock_.Lock();
  int notZeroCloud1=0;
  int notZeroCloud2=0;
  int notZeroSsd=0;
  for (auto it = temp_sst_iotimes.begin(); it != temp_sst_iotimes.end(); it++) {
    if (cloudManager.sstMap.find(it->first) != cloudManager.sstMap.end()) {
      auto p = cloudManager.sstMap[it->first];
      p->get_times =( (p->get_times) + it->second)/2 ;
      if(p->get_times||it->second)
      {
        // fprintf(stderr,"cloud:get_times:%d last1s times:%d\n",p->get_times,it->second);
        notZeroCloud1++;
      }
    } else if (ssdManager.sstMap.find(it->first) != ssdManager.sstMap.end()) {
      auto p = ssdManager.sstMap[it->first];
      p->get_times = ((p->get_times) + it->second)/2;
      if(p->get_times||it->second)
      {
        // fprintf(stderr,"ssd:get_times:%d last1s times:%d\n",p->get_times,it->second);
        notZeroSsd++;
      }
    } else {
      cloudManager.sstMap[it->first] = new SstTemp(it->first, it->second/2);
      if(it->second)
      {
        // fprintf(stderr,"cloud:last1s times:%d\n",it->second);
        notZeroCloud2++;
      }
    }
  }
  for (auto it = cloudManager.sstMap.begin(); it != cloudManager.sstMap.end();
       it++) {
    if (temp_sst_iotimes.find(it->first) == temp_sst_iotimes.end()) {
      auto p = it->second;
      auto key = it->first;
      p->get_times = (p->get_times)/2 ;
    }
  }
  for (auto it = ssdManager.sstMap.begin(); it != ssdManager.sstMap.end();
       it++) {
    if (temp_sst_iotimes.find(it->first) == temp_sst_iotimes.end()) {
      auto p = it->second;
      auto key = it->first;
      p->get_times = (p->get_times)/2 ;
    }
  }
  calcuTimes++;
  if(calcuTimes>=10)
  {
    calcuTimes=0;
    uint64_t size=CacheSize-(ssdManager.sstMap.size()*blkSize);
    options_->block_cache->SetCapacity(size);
    fprintf(stderr,"blkcache size=%d\n",size);
  }
  if(tempLog)
  {
    fprintf(tempLog,"prefetch hit:%d  prefetchAll:%d\nblkcache hit:%d  blkcacheAll:%d\n",hit_times,all_times,blkcache_hit_times,blkcache_all_times);
    fprintf(tempLog,"KVcache hit:%d  KVcacheAll:%d\n",KVcache_hit_times,KVcache_all_times);
    fprintf(tempLog,"notZeroCloud1:%d notZeroCloud2:%d notZeroSsd:%d\n",notZeroCloud1,notZeroCloud2,notZeroSsd);
    fflush(tempLog);
    hit_times=0;
    all_times=0;
    blkcache_hit_times=0;
    blkcache_all_times=0;
    KVcache_hit_times=0;
    KVcache_all_times=0;

  }
  // cloudManager.sortSst();
  // ssdManager.sortSst();
  lock_.Unlock();
}

size_t Prefetcher::TryGetFromPrefetcher(uint64_t sst_id, uint64_t offset,
                                        size_t n, char *scratch) {
  static Prefetcher &i = _GetInst();
  return i._TryGetFromPrefetcher(sst_id, offset, n, scratch);
}

size_t Prefetcher::_Prefetcher2BlocksFromMem(uint64_t key, uint64_t offset,
                                             size_t n, char *scratch) {
  lock_mem.Lock();
  char *temp = sst_blocks[key];
  size_t errorFlag = n + 1;
  if (temp == nullptr) {
    fprintf(stderr, "err\n");
    lock_mem.Unlock();
    return errorFlag;
  }
  size_t left = 0;
  int num1 = blkSize - offset;
  if (num1) {
    memcpy(scratch, temp + offset, num1);
  }
  char *temp2 = sst_blocks[key + 1];
  if (temp == nullptr) {
    fprintf(stderr, "err\n");
    lock_mem.Unlock();
    return errorFlag;
  }
  int num2 = n - num1;
  if (num2) {
    memcpy(scratch+num1, temp2, num2);
  }

  lock_mem.Unlock();
  return left;
}

size_t Prefetcher::_PrefetcherFromMem(uint64_t key, uint64_t offset, size_t n,
                                      char *scratch) {
  lock_mem.Lock();
  char *temp = sst_blocks[key];
  size_t errorFlag = n + 1;
  if (temp == nullptr) {
    fprintf(stderr, "err\n");
    lock_mem.Unlock();
    return errorFlag;
  }
  size_t left = 0;
  memcpy(scratch, temp + offset, n);
  lock_mem.Unlock();
  return left;
}
size_t Prefetcher::_PrefetcherOneFile(uint64_t key, uint64_t offset, size_t n,
                                      char *scratch) {
  size_t errorFlag = n + 1;
  std::string fname = TableFileName(db_paths, key, 1);
  int fd = open(fname.c_str(), O_RDONLY | O_DIRECT);
  if (fd < 0) {
    fprintf(stderr, "_TryGetFromPrefetcher file open error\n");
    return errorFlag;
  }
  Status s;
  ssize_t r = -1;
  size_t left = n;
  char *ptr = scratch;
  while (left > 0) {
    r = pread(fd, ptr, left, static_cast<off_t>(offset));
    if (r <= 0) {
      if (r == -1 && errno == EINTR) {
        continue;
      }
      break;
    }
    ptr += r;
    offset += r;
    left -= r;
  }
  if (r < 0) {
    // An error: return a non-ok status
    fprintf(stderr, "IOerror\n");
    close(fd);
    return errorFlag;
  }
  close(fd);
  return left;
}
size_t Prefetcher::_PrefetcherTwoFiles(uint64_t key, uint64_t offset, size_t n,
                                       char *scratch) {
  fprintf(stderr, "_PrefetcherTwoFiles begin\n");
  size_t errorFlag = n + 1;
  std::string fname1 = TableFileName(db_paths, key, 1);
  std::string fname2 = TableFileName(db_paths, key + 1, 1);
  int fd1 = open(fname1.c_str(), O_RDONLY);
  int fd2 = open(fname1.c_str(), O_RDONLY);
  if (fd1 < 0 || fd2 < 0) {
    fprintf(stderr, "_TryGetFromPrefetcher file open error\n");
    return false;
  }
  Status s;
  ssize_t r = -1;
  size_t left = blkSize - offset;
  char *ptr = scratch;
  while (left > 0) {
    r = pread(fd1, ptr, left, static_cast<off_t>(offset));
    if (r <= 0) {
      if (r == -1 && errno == EINTR) {
        continue;
      }
      break;
    }
    ptr += r;
    offset += r;
    left -= r;
  }
  if (r < 0 || left != 0) {
    // An error: return a non-ok status
    fprintf(stderr, "IOerror\n");
    close(fd1);
    close(fd2);
    return errorFlag;
  }
  left = n - (blkSize - offset);
  offset = 0;
  while (left > 0) {
    r = pread(fd2, ptr, left, static_cast<off_t>(offset));
    if (r <= 0) {
      if (r == -1 && errno == EINTR) {
        continue;
      }
      break;
    }
    ptr += r;
    offset += r;
    left -= r;
  }
  if (r < 0) {
    // An error: return a non-ok status
    fprintf(stderr, "IOerror\n");
    close(fd1);
    close(fd2);
    return errorFlag;
  }
  close(fd1);
  close(fd2);
  fprintf(stderr, "_PrefetcherTwoFiles end\n");
  return left;
}
size_t Prefetcher::_TryGetFromPrefetcher(uint64_t sst_id, uint64_t offset,
                                         size_t n, char *scratch) {
                                          // fprintf(stderr,"size:%d\n",n);
  all_times++;
  all_prefetch_all_times++;
  size_t errorFlag = n + 1;
  uint64_t key = sst_id * 100000 + offset / (blkSize);
  offset = offset % (blkSize);
  if (offset + n > blkSize) {
    if (n<=blkSize&&ssdManager.sstMap.find(key) != ssdManager.sstMap.end() &&
        ssdManager.sstMap.find(key + 1) != ssdManager.sstMap.end()) {
      // fprintf(stderr, "need 2 blocks size==%d\n",n);
      size_t re = errorFlag;
      re = _Prefetcher2BlocksFromMem(key, offset, n, scratch);
      if (re != errorFlag) {
        // fprintf(stderr, "hit\n");
        hit_times++;
        all_prefetch_hit_times++;
      }
      return re;
    }
    return errorFlag;
  } else {
    if (ssdManager.sstMap.find(key) == ssdManager.sstMap.end()) {
      return errorFlag;
    }
    size_t re = errorFlag;
    // re=_PrefetcherOneFile(key, offset, n, scratch);
    re = _PrefetcherFromMem(key, offset, n, scratch);
    if (re != errorFlag) {
      // fprintf(stderr, "hit\n");
      hit_times++;
      all_prefetch_hit_times++;
    }
    return re;
  }
}

Prefetcher &Prefetcher::_GetInst() {
  static Prefetcher i;
  return i;
}
void Prefetcher::Init(DBImpl *impl, bool doPrefetch_) {
  fprintf(stderr, "Prefetcher Init \n");
  static Prefetcher &i = _GetInst();
  i._Init(impl, doPrefetch_);
}

void Prefetcher::SetOptions(BlockBasedTableOptions* options)
{
  static Prefetcher &i = _GetInst();
  if(!i.options_)
  {
    i.options_=options;
  }
}

void Prefetcher::_Init(DBImpl *impl, bool doPrefetch_) {
  fprintf(stderr, "Prefetcher Init\n");
  if (inited) {
    return;
  }
  cache=new LRUCache(350*1024);
  inited = true;
  impl_ = impl;

  if(tempLog==nullptr)
  {
    tempLog=fopen("./hit_ratio.log","a+");
  }

  if (logRWlat) {
    hdr_init(1, INT64_C(3600000000), 3, &hdr_last_1s_read);
    hdr_init(1, INT64_C(3600000000), 3, &hdr_last_1s_write);
    hdr_init(1, INT64_C(3600000000), 3, &hdr_last_1s_size);
    readiops = 0;
    writeiops = 0;
    tiktoks = 0;
    logFp_read = std::fopen("./ssd_rlat.log", "w+");
    logFp_write = std::fopen("./ssd_wlat.log", "w+");
    logFp_size = std::fopen("./ssd_size.log", "w+");
    logFp_prefetch_times = std::fopen("./prefetch_times.log", "w+");
    logFp_limiter_time = std::fopen("./limite_times.log", "w+");
    fprintf(logFp_read,
            "#type mean   25th   50th   75th    90th    99th    99.9th    IOPS "
            "rwIOPS\n");
    fprintf(
        logFp_write,
        "#type mean   25th   50th   75th    90th    99th    99.9th    IOPS\n");
    fprintf(logFp_size,
            "#type mean   25th   50th   75th    90th    99th    99.9th ");
    fprintf(logFp_prefetch_times, "#time  prefetch_times");
    fprintf(logFp_limiter_time,
            "#time prefetch  compaction  flush&l0compaction\n");
    tiktok_start = now();
    prefetch_start = now();
    limiter_star_time = now();
  }
  std::thread t(caluateSstHeatThread);
    t.detach();
  if (doPrefetch_) {
    int ret;
    ret = posix_memalign((void **)&buf_, 512, blkSize);
    if (ret) {
      fprintf(stderr, "posix_memalign failed");
      exit(1);
    }

    

    std::thread t2(prefetchThread);
    t2.detach();
  }
  fprintf(stderr, "Prefetcher Init end\n");
}

int64_t Prefetcher::now() {
  struct timeval tv;
  ::gettimeofday(&tv, 0);
  int64_t seconds = tv.tv_sec;
  return seconds * NS_PER_SECOND + tv.tv_usec * NS_PER_USECOND;
}

void Prefetcher::SstRead(uint64_t sst_id, uint64_t offset, size_t size,
                         bool isGp2)  //更新sst的读写次数(热度)
{
  // fprintf(stderr,"size=%d\n",size);
  if (sst_id == 0) {
    return;
  }
  static Prefetcher &i = _GetInst();
  i._SstRead(sst_id, offset, size, isGp2);
}

void Prefetcher::_SstRead(uint64_t sst_id, uint64_t offset, size_t size,
                          bool isGp2) {
  int blk_num = offset / (blkSize);

  uint64_t key = sst_id * 100000 + blk_num;
  // fprintf(stderr, "sst: %lu offset: %lu  key:%lu \n", sst_id, offset, key);
  lock_sst_io.Lock();
  sst_iotimes[key]++;
  if ((offset % (blkSize)) + size >= (blkSize)) {
    sst_iotimes[key + 1]++;
  }
  lock_sst_io.Unlock();
}

void Prefetcher::RecordTime(int op, uint64_t tx_xtime,
                            size_t size)  // op : 1read 2write
{
  static Prefetcher &i = _GetInst();
  i._RecordTime(op, tx_xtime, size);
}

void Prefetcher::log_prefetch_times(int time, int times) {
  // fprintf(f_hdr_hiccup_output_, "mean     95th     99th     99.99th   IOPS");
  fprintf(logFp_prefetch_times, "%-8ld  %-8ld\n", log_time + time, times);
  log_time += time;
  fflush(logFp_prefetch_times);
}

void Prefetcher::latency_hiccup_read(uint64_t iiops, uint64_t alliops) {
  // fprintf(f_hdr_hiccup_output_, "mean     95th     99th     99.99th   IOPS");
  fprintf(logFp_read,
          "read %-11.2lf %-8ld %-8ld %-8ld %-8ld %-8ld %-8ld %-8ld %-8ld\n",
          hdr_mean(hdr_last_1s_read),
          hdr_value_at_percentile(hdr_last_1s_read, 25),
          hdr_value_at_percentile(hdr_last_1s_read, 50),
          hdr_value_at_percentile(hdr_last_1s_read, 75),
          hdr_value_at_percentile(hdr_last_1s_read, 90),
          hdr_value_at_percentile(hdr_last_1s_read, 99),
          hdr_value_at_percentile(hdr_last_1s_read, 99.9), iiops, alliops);
  hdr_reset(hdr_last_1s_read);
  fflush(logFp_read);
}

void Prefetcher::latency_hiccup_write(uint64_t iiops) {
  // fprintf(f_hdr_hiccup_output_, "mean     95th     99th     99.99th   IOPS");
  fprintf(logFp_write,
          "write %-11.2lf %-8ld %-8ld %-8ld %-8ld %-8ld %-8ld %-8ld\n",
          hdr_mean(hdr_last_1s_write),
          hdr_value_at_percentile(hdr_last_1s_write, 25),
          hdr_value_at_percentile(hdr_last_1s_write, 50),
          hdr_value_at_percentile(hdr_last_1s_write, 75),
          hdr_value_at_percentile(hdr_last_1s_write, 90),
          hdr_value_at_percentile(hdr_last_1s_write, 99),
          hdr_value_at_percentile(hdr_last_1s_write, 99.9), iiops);
  hdr_reset(hdr_last_1s_write);
  fflush(logFp_write);
}

void Prefetcher::latency_hiccup_size() {
  // fprintf(f_hdr_hiccup_output_, "mean     95th     99th     99.99th   IOPS");
  fprintf(logFp_size, "size %-11.2lf %-8ld %-8ld %-8ld %-8ld %-8ld %-8ld\n",
          hdr_mean(hdr_last_1s_size),
          hdr_value_at_percentile(hdr_last_1s_size, 25),
          hdr_value_at_percentile(hdr_last_1s_size, 50),
          hdr_value_at_percentile(hdr_last_1s_size, 75),
          hdr_value_at_percentile(hdr_last_1s_size, 90),
          hdr_value_at_percentile(hdr_last_1s_size, 99),
          hdr_value_at_percentile(hdr_last_1s_size, 99.9));
  hdr_reset(hdr_last_1s_size);
  fflush(logFp_size);
}
void Prefetcher::_RecordTime(int op, uint64_t tx_xtime,
                             size_t size)  // op : 1read 2write
{
  if (!logRWlat) {
    return;
  }
  lock_rw.Lock();
  if (tx_xtime > 3600000000) {
    fprintf(stderr, "too large tx_xtime");
    return;
  }
  int num = size / (256 * 1024);
  if (size % (256 * 1024) != 0) {
    num++;
  }
  if (op == 1) {
    // hdr_record_value(hdr_last_1s_read, tx_xtime);
    // hdr_record_value(hdr_last_1s_size, size);
    readiops += num;
  } else if (op == 2) {
    // hdr_record_value(hdr_last_1s_write, tx_xtime);
    writeiops += num;
  }
  tiktoks = now() - tiktok_start;
  if (tiktoks >= 1000000000) {
    if (logRWlat) {
      latency_hiccup_read(readiops, readiops + writeiops);
      latency_hiccup_write(writeiops);
      latency_hiccup_size();
    }
    if (lastIOPS.size() < 10) {
      lastIOPS.push_back(readiops);
    } else {
      lastIOPS.pop_front();
      lastIOPS.push_back(readiops);
      int allIOPS = 0;
      for (auto i = lastIOPS.begin(); i != lastIOPS.end(); i++) {
        allIOPS += *i;
      }
      bool t = pauseComapaction.load();
      if (allIOPS / lastIOPS.size() >= IOPS_MAX * 0.9 && !t) {
        if (!paused) {
          Status status = impl_->PauseBackgroundWork();
          if (status == Status::OK()) {
            pauseComapaction.store(true);
            paused = true;
          }
        }
      } else if (allIOPS / lastIOPS.size() < IOPS_MAX * 0.9 && t) {
        if (paused) {
          impl_->ContinueBackgroundWork();
          paused = false;
          pauseComapaction.store(false);
        }
      }
    }

    tiktok_start = now();
    readiops = 0;
    writeiops = 0;
    tiktoks = 0;
  }

  lock_rw.Unlock();
}
bool Prefetcher::getCompactionPaused() { return pauseComapaction.load(); }

}  // namespace rocksdb