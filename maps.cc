#include <sys/mman.h>
#include <fcntl.h>
#include <string.h>
#include <unistd.h>
#include <stdint.h>
#include <sys/time.h>

#include <gflags/gflags.h>

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>

#include <vector>
#include <string>
#include <algorithm>

using std::vector;
using std::string;

static struct {
  const char *file;
  const char *stats_file;
  uint64_t file_size;
  int populate;
  int nthreads;
  int nreaders;
  int nmappers;
  int runtime;
} options = {
    .file = "/mnt/mmap.dat",
    .stats_file = "timing.csv",
    .file_size = 200ul * 1024 * 1024 * 1024,
    .populate = 0,
    .nthreads = 10000,
    .nreaders = 10,
    .nmappers = 0,
    .runtime = 60,
};

DEFINE_string(map_file, options.file, "File to mmap() and access");
DEFINE_string(out, options.stats_file, "Path to write timing statistics");
DEFINE_uint64(gb, options.file_size >> 30, "Map file size, in GiB");

DEFINE_bool(populate, !!options.populate,
            "Pre-populate the map file, don't just ftruncate()");

DEFINE_int32(threads, options.nthreads, "Number of idle threads");
DEFINE_int32(readers, options.nreaders, "Number of reader threads");
DEFINE_int32(mappers, options.nmappers, "Number of mappers threads");

DEFINE_int32(time, options.runtime, "Seconds to run");

const int kWindow = 1000;
const int kWindowUS = 1000 * 1000;

uint64_t time_us();

struct window {
  uint64_t start;
  uint64_t avg;
  uint64_t p50;
  uint64_t p90;
  uint64_t p99;
};

class thread_stats {
public:
  thread_stats() : start_(0), window_start_(0), samples_(), sample_(0) {
    data_.reserve(1000);
  };

  void flush() {
    window w;
    std::sort(&samples_[0], &samples_[sample_ - 1]);
    uint64_t sum = 0;
    for (int i = 0; i < sample_; ++i)
      sum += samples_[i];
    w.start = window_start_;
    w.avg = sum / sample_;
    w.p50 = samples_[sample_ / 2];
    w.p90 = samples_[sample_ * 9 / 10];
    w.p99 = samples_[sample_ * 99 / 100];
    data_.push_back(w);
    sample_ = 0;
  }

  void measure_begin() {
    start_ = time_us();
    if (sample_ == 0)
      window_start_ = start_;
    asm("mfence" : : : "memory");
  }
  void measure_end() {
    asm("mfence" : : : "memory");
    uint64_t end = time_us();
    uint64_t t = end - start_;
    samples_[sample_++] = t;
    if (sample_ == kWindow || (end - window_start_) > kWindowUS) {
      flush();
    }
  }

  const vector<window> data() {
    if (sample_ > 0)
      flush();
    return data_;
  }

private:
  uint64_t start_;
  uint64_t window_start_;
  uint64_t samples_[kWindow];
  int sample_;

  std::vector<window> data_;
};

void panic(const char *why) {
  perror(why);
  exit(1);
}

const int PAGE_SIZE = 4096;

void populate(int fd) {
  char buf[PAGE_SIZE];
  for (uint64_t i = 0; i < options.file_size / PAGE_SIZE; ++i) {
    memset(buf, 0, PAGE_SIZE);
    if (write(fd, buf, sizeof(buf)) != sizeof(buf)) {
      panic("write");
    }
  }
}

uint64_t time_us() {
  timeval now;
  gettimeofday(&now, NULL);
  return now.tv_sec * 1000000 + now.tv_usec;
}

struct thread_args {
  pthread_t *self;
  void *map;
  thread_stats *stats;
};

void *accessor(void *p) {
  thread_args *args = static_cast<thread_args *>(p);
  struct random_data rand = {};
  char randbuf[32];
  initstate_r((uintptr_t)args->self, &randbuf[0], sizeof(randbuf), &rand);

  uint64_t start = time_us();
  int total = 0;
  while (true) {
    for (int i = 0; i < 1000; ++i) {
      int32_t off;
      random_r(&rand, &off);
      uint8_t *p = (uint8_t *)args->map +
                   PAGE_SIZE * (off % (options.file_size / PAGE_SIZE));
      args->stats->measure_begin();
      total += *p;
      args->stats->measure_end();
    }
    if ((time_us() - start) > uint64_t(options.runtime) * 1000 * 1000)
      break;
  }
  return (void *)(uintptr_t)total;
}

void *donothing(void *) __attribute__((noreturn));
void *donothing(void *) {
  while (true) {
    pause();
  }
}

void *dommap(void *) __attribute__((noreturn));
void *dommap(void *) {
  while (true) {
    void *p = mmap(NULL, PAGE_SIZE, PROT_READ, MAP_ANON | MAP_PRIVATE, -1, 0);
    if (p == (void *)MAP_FAILED)
      panic("mmap");
    munmap(p, PAGE_SIZE);
  }
}

int main(int argc, char **argv) {
  google::SetUsageMessage("Usage: " + string(argv[0]) + "<options>");
  google::ParseCommandLineFlags(&argc, &argv, true);

  options.file = FLAGS_map_file.c_str();
  options.stats_file = FLAGS_out.c_str();
  options.file_size = FLAGS_gb << 30;

  options.populate = FLAGS_populate;

  options.nthreads = FLAGS_threads;
  options.nreaders = FLAGS_readers;
  options.nmappers = FLAGS_mappers;

  options.runtime = FLAGS_time;

  int fd = open(options.file, O_RDWR | O_CREAT, 0644);
  if (fd < 0) {
    panic("open");
  }
  if (ftruncate(fd, options.file_size) < 0) {
    panic("ftruncate");
  }

  if (options.populate) {
    printf("populating file...\n");
    populate(fd);
  }

  printf("mapping data...\n");
  void *map = mmap(NULL, options.file_size, PROT_READ, MAP_SHARED, fd, 0);
  if (map == (void *)MAP_FAILED)
    panic("mmap");
  madvise(map, options.file_size, MADV_RANDOM);

  printf("launching threads...\n");
  for (int i = 0; i < options.nthreads; ++i) {
    pthread_t dummy;
    if (pthread_create(&dummy, NULL, donothing, NULL) < 0)
      panic("pthread_create");
  }

  for (int i = 0; i < options.nmappers; ++i) {
    pthread_t dummy;
    if (pthread_create(&dummy, NULL, dommap, NULL) < 0)
      panic("pthread_create");
  }

  pthread_t threads[options.nreaders];
  struct thread_args args[options.nreaders];
  for (int i = 0; i < options.nreaders; ++i) {
    args[i].self = &threads[i];
    args[i].map = map;
    args[i].stats = new thread_stats();
    if (pthread_create(&threads[i], NULL, accessor, &args[i]) < 0)
      panic("pthread_create");
  }
  printf("running...\n");

  for (int i = 0; i < options.nreaders; ++i)
    pthread_join(threads[i], NULL);

  printf("dumping stats...\n");
  FILE *out = fopen(options.stats_file, "w");
  for (int i = 0; i < options.nreaders; ++i) {
    for (window w : args[i].stats->data()) {
      fprintf(out, "%d,%lu,%lu,%lu,%lu,%lu\n", i, w.start, w.avg, w.p50, w.p90,
              w.p99);
    }
  }
  fclose(out);
}
