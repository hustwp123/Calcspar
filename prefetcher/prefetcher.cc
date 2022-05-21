#include "prefetcher/prefetcher.h"
#include "util/token_limiter.h"

namespace rocksdb {

std::vector<DbPath> db_paths = {
    {"/home/ubuntu/gp2_150g_1", 60l * 1024 * 1024 * 1024},
    {"/home/ubuntu/ssd_150g", 60l * 1024 * 1024 * 1024},
};

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
  i._Prefetcher();
}

void Prefetcher::CaluateSstHeat() {
  static Prefetcher &i = _GetInst();
  i._CaluateSstHeat();
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
  fromKey=cloudManager.getMax();
  if (ssdManager.sstMap.size() < MAXSSTNUM)  // ssd中未放满
  {
    lock_.Unlock();
  } else  // ssd中已放满
          // 需比较ssd中最冷的sst_blk的热度和cloud中最热的sst_blk的热度 进行替换
  {
    outKey=ssdManager.getMin();
    if ((ssdManager.sstMap[outKey]->get_times) <
        (cloudManager.sstMap[fromKey]->get_times)) {
      lock_.Unlock();
    } else {
      lock_.Unlock();
      return;
    }
  }
  uint64_t fromSstId = fromKey / 10000;
  uint64_t blk_num = fromKey % 10000;
  if(fromSstId==0)
  {
    fprintf(stderr,"err sstid==0 key==%lu gettimes= %lu\n",fromKey,cloudManager.sstMap[fromKey]->get_times);
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
  int readfd = open(old_fname.c_str(), O_RDONLY);
  if (readfd == -1) {
    lock_.Lock();
    // fprintf(stderr, "file open error1 sstid=%lu\n", fromSstId);
    SstTemp *t = cloudManager.sstMap[fromSstId];
    cloudManager.sstMap.erase(fromSstId);
    delete t;
    lock_.Unlock();
    return;
  }
  int writefd = open(new_fname.c_str(), O_WRONLY | O_CREAT, S_IRWXU);
  if (writefd == -1) {
    fprintf(stderr, "file open error2 sstid=%lu\n", fromSstId);
    close(readfd);
    return;
  }
  char buf[256 * 1024];
  uint64_t offset = blk_num * 256 * 1024;
  TokenLimiter::RequestDefaultToken(Env::IOSource::IO_SRC_PREFETCH, TokenLimiter::kRead);
  int ret = pread(readfd, buf, 256 * 1024, offset);
  if (ret == -1) {
    fprintf(stderr, "pread error\n");
    close(readfd);
    close(writefd);
    return;
  }
  ret = pwrite(writefd, buf, 256 * 1024, 0);
  if (ret != 256 * 1024) {
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
  for (auto it = temp_sst_iotimes.begin(); it != temp_sst_iotimes.end(); it++) {
    if (cloudManager.sstMap.find(it->first) != cloudManager.sstMap.end()) {
      auto p = cloudManager.sstMap[it->first];
      p->get_times = (p->get_times) * 0.2 + it->second * 0.8;
    } else if (ssdManager.sstMap.find(it->first) != ssdManager.sstMap.end()) {
      auto p = ssdManager.sstMap[it->first];
      p->get_times = (p->get_times) * 0.2 + it->second * 0.8;
    } else {
      cloudManager.sstMap[it->first] = new SstTemp(it->first, it->second * 0.8);
    }
  }
  // if (doPrefetch) {
  //   _Prefetcher();
  // }
}

size_t Prefetcher::TryGetFromPrefetcher(uint64_t sst_id, uint64_t offset,
                                        size_t n, char *scratch) {
  static Prefetcher &i = _GetInst();
  return i._TryGetFromPrefetcher(sst_id, offset, n, scratch);
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
  fprintf(stderr,"_PrefetcherTwoFiles begin\n");
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
  size_t left = 256*1024-offset;
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
  if (r < 0||left!=0) {
    // An error: return a non-ok status
    fprintf(stderr, "IOerror\n");
    close(fd1);
    close(fd2);
    return errorFlag;
  }
  left=n-(256*1024-offset);
  offset=0;
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
  fprintf(stderr,"_PrefetcherTwoFiles end\n");
  return left;
}
size_t Prefetcher::_TryGetFromPrefetcher(uint64_t sst_id, uint64_t offset,
                                         size_t n, char *scratch) {
  size_t errorFlag = n + 1;
  uint64_t key = sst_id * 10000 + offset / (256 * 1024);
  offset = offset % (256 * 1024);
  if (offset + n > 256 * 1024) {
    // fprintf(stderr, "need 2 more blocks\n");
    return errorFlag;
  } 
  // else if (offset + n > 256 * 1024) {
  //   lock_.Lock();
  //   if (ssdManager.sstMap.find(key) == ssdManager.sstMap.end() ) {
  //     lock_.Unlock();
  //     // fprintf(stderr,"2 blocks not found\n");
  //     return errorFlag;
  //   }
  //   if (ssdManager.sstMap.find(key+1) == ssdManager.sstMap.end() ) {
  //     lock_.Unlock();
  //     fprintf(stderr,"2 blocks not found\n");
  //     return errorFlag;
  //   }
  //   lock_.Unlock();
  //   return _PrefetcherTwoFiles(key,offset,n,scratch);
  // } 
  else {
    lock_.Lock();
    if (ssdManager.sstMap.find(key) == ssdManager.sstMap.end()) {
      lock_.Unlock();
      return errorFlag;
    }
    lock_.Unlock();
    return _PrefetcherOneFile(key,offset,n,scratch);
  }
}

Prefetcher &Prefetcher::_GetInst() {
  static Prefetcher i;
  return i;
}
void Prefetcher::Init() {
  printf("Prefetcher Init \n");
  static Prefetcher &i = _GetInst();
  i._Init();
}
void Prefetcher::_Init() {
  fprintf(stderr, "Prefetcher Init\n");
  if (inited) {
    return;
  }
  inited = true;
  std::thread t(caluateSstHeatThread);
  t.detach();

  std::thread t2(prefetchThread);
  t2.detach();
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
  if (sst_id == 0) {
    return;
  }
  static Prefetcher &i = _GetInst();
  i._SstRead(sst_id, offset, size, isGp2);
}

void Prefetcher::_SstRead(uint64_t sst_id, uint64_t offset, size_t size,
                          bool isGp2) {
  int blk_num = offset / (256 * 1024);

  uint64_t key = sst_id * 10000 + blk_num;
  // fprintf(stderr, "sst: %lu offset: %lu  key:%lu \n", sst_id, offset, key);
  lock_sst_io.Lock();
  sst_iotimes[key]++;
  if ((offset % (256 * 1024)) + size >= (256 * 1024)) {
    sst_iotimes[key + 1]++;
  }
  lock_sst_io.Unlock();
}

}  // namespace rocksdb