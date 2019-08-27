#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdint.h>
#include <string.h>
#include <pthread.h>
#include <sched.h>
#include <memory>
#include <thread>
#include "kv_intf.h"
#include "kv_string.h"

#ifdef SLOW_DEBUG
  #include <stdio.h>
  #include <sys/time.h>
  #include <atomic>
  #include <vector>
  #define DEBUG(format,...)     printf("[DEBUG] " format "\n", ##__VA_ARGS__)
  #define ERROR(format,...)     printf("[ERROR] %s:%s[%d] " format "\n", __FILE__, __FUNCTION__, __LINE__, ##__VA_ARGS__)
  #define TIMEVAL_DEFINE(x)     struct timeval (x)
  #define TIMEVAL_GET(x)        gettimeofday(&(x), NULL)
  #define TIMEVAL_DEFINEGET(x)  struct timeval (x); gettimeofday(&(x), NULL)
  #define TIMEVAL_VAL(tv1, tv2) (((long long)(tv2).tv_sec - (tv1).tv_sec) * 1000000 + (tv2).tv_usec - (tv1).tv_usec)
  static std::atomic<int32_t> g_count_local_cache(0);
  static std::atomic<int32_t> g_count_bio_cache(0);
  static std::atomic<int32_t> g_count_bio_reverse_cache(0);
  static std::atomic<int32_t> g_count_unsafe_dio(0);
  static std::atomic<int32_t> g_count_dio_slow(0);
#else
  #define DEBUG(format,...)
  #define ERROR(format,...)
  #define TIMEVAL_DEFINE(x)
  #define TIMEVAL_GET(x)
  #define TIMEVAL_DEFINEGET(x)
  #define TIMEVAL_VAL(tv1, tv2)
#endif

#define kAmountMaxPerThread 400 * 10000
#define kThreadMax          16
#define kSize8B             8
#define kSize4K             4096LL
#define kSizeOfIndex        16LL
#define kDataBufferSize     512 * 1024

static inline bool FileExists(const char *path)
{
    return access(path, F_OK) == 0;
}

static inline int64_t GetFileLength(const char *path)
{
    struct stat stat_buf;

    return stat(path, &stat_buf) == 0 ? stat_buf.st_size : -1;
}

static inline std::string IntToString4(const int n)
{
    std::string ret = "";

    if (n < 1000)
    {
        ret += '0';
        if (n < 100)
        {
            ret += '0';
            if (n < 10)
            {
                ret += '0';
            }
        }
    }

    return ret + std::to_string(n);
}

#define kHashBit    22
static const int32_t g_hash_len = 1 << kHashBit;
static const int32_t g_hash_mod = g_hash_len - 1;
static int32_t g_hash_table[kThreadMax][g_hash_len];
static int32_t g_kv_count[kThreadMax];
static char *g_cache;
static char *g_reverse_cache;

#define kLocalPerThread    60000
//#define kLocalPerThread    57344
static char *g_local[kThreadMax];

class __DB
{
public:
    __DB():
        hash_table_(NULL),
        buffer_(NULL),
        buffer_pos_(NULL),
        read_fd_dio(-1),
        write_fd_(-1),
        cache_pos_(0),
        cache_seqid_(-1),
        cache_len_(0),
        reverse_cache_pos_(0),
        reverse_cache_seqid_(-1),
        is_init_(false),
        cache_dropped_(false),
        reverse_cache_dropped_(false)
    {
    }

    ~__DB()
    {
        if (is_init_)
        {
            if (*buffer_pos_ > 0)
                flush();
        }
    }

    void build(const char *dir, const int8_t thid)
    {
        std::string file_name;
        int fd;

        //-----------index
        hash_table_ = g_hash_table[thid];
        cache_ = g_cache + thid * kDataBufferSize;
        reverse_cache_ = g_reverse_cache + thid * kDataBufferSize;
        local_ = g_local[thid];
        kv_count_ = g_kv_count + thid;

        //----------data
        file_name = std::string(dir) + "/DATA." + IntToString4(thid);
        fd = open(file_name.c_str(), O_RDONLY | O_LARGEFILE | O_NOATIME | O_CREAT | O_DIRECT, 0644);
        read_fd_dio = fd;

        fd = open(file_name.c_str(), O_WRONLY | O_LARGEFILE | O_APPEND | O_DIRECT);
        write_fd_ = fd;

        //-----------data buffer
        file_name = std::string(dir) + "/BUFFER.DATA." + IntToString4(thid);
        fd = open(file_name.c_str(), O_RDWR | O_CREAT, 0644);
        bool is_new = false;
        if (GetFileLength(file_name.c_str()) != kSize4K + kDataBufferSize)
        {
            is_new = true;
            ftruncate(fd, kSize4K + kDataBufferSize);
        }

        void *ptr = mmap(NULL, kSize4K + kDataBufferSize, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
        close(fd);
        if (is_new)
            memset(ptr, 0, kSize4K + kDataBufferSize);

        buffer_pos_ = (uint32_t *)ptr;
        buffer_ = (char *)ptr + kSize4K;

        DEBUG("__DB build succ %s thid_=%d", dir, thid);
    }

    void init()
    {
        cache_pos_ = 0;
        cache_seqid_ = -1;
        cache_len_ = 0;
        reverse_cache_pos_ = 0;
        reverse_cache_seqid_ = -1;
        cache_dropped_ = false;
        reverse_cache_dropped_ = false;
        is_init_ = true;
    }

    void WAL(const char *keybuf, const char *valbuf)
    {
        int32_t seqid = (*kv_count_)++;
        if (seqid < kLocalPerThread)
            memcpy(local_ + seqid * kSize4K, valbuf, kSize4K);
        else
        {
            if (*buffer_pos_ == kDataBufferSize)
                flush();

            memcpy(buffer_ + *buffer_pos_, valbuf, kSize4K);
            *buffer_pos_ += kSize4K;
        }

        hash_table_[(*((uint64_t *)keybuf)) & g_hash_mod] = seqid;
    }

    int find(const uint64_t key_i64, KVString& val)
    {
        if (*buffer_pos_ > 0)
            flush();

        int32_t seqid = hash_table_[key_i64 & g_hash_mod];
        if (seqid < 0)
            return -1;

        const char *valbuf = val.Buf();
        const char *buf;
        if (seqid < kLocalPerThread)
        {
#ifdef SLOW_DEBUG
            g_count_local_cache++;
#endif
            buf = local_ + seqid * kSize4K;
        }
        else
        {
            seqid -= kLocalPerThread;
            buf = get_from_cache(seqid);
        }

        if (buf)
        {
            if (val.Size() == kSize4K)
                memcpy(const_cast<char *>(valbuf), buf, kSize4K);
            else
                val = KVString(buf, kSize4K);
        }
        else if (val.Size() == kSize4K && (((uint64_t)valbuf & 0xfff) == 0))
            find_DIO_unsafe(seqid, const_cast<char *>(valbuf));
        else
            val.Reset(find_DIO_slow(seqid), kSize4K);

        return 0;
    }

private:
    void flush() const
    {
        write(write_fd_, buffer_, *buffer_pos_);
        *buffer_pos_ = 0;
    }

    const char *get_from_cache(int32_t seqid)
    {
        do
        {
            if (cache_dropped_)
                break;

            if (cache_seqid_ < 0)
            {
                cache_len_ = pread64(read_fd_dio, cache_, kDataBufferSize, 0);
                if (cache_len_ <= 0)
                {
                    cache_dropped_ = true;
                    break;
                }

                cache_seqid_ = 0;
                cache_pos_ = 0;
            }

            if (seqid != cache_seqid_)
            {
                cache_dropped_ = true;
                break;
            }

            if (cache_pos_ == cache_len_)
            {
                cache_len_ = pread64(read_fd_dio, cache_, kDataBufferSize, cache_seqid_ * kSize4K);
                if (cache_len_ <= 0)
                {
                    cache_dropped_ = true;
                    break;
                }

                cache_pos_ = 0;
            }

#ifdef SLOW_DEBUG
            g_count_bio_cache++;
#endif
            cache_seqid_++;
            cache_pos_ += kSize4K;
            return cache_ + cache_pos_ - kSize4K;
        } while (0);

        do
        {
            if (reverse_cache_dropped_)
                break;

            if (reverse_cache_seqid_ < 0)
            {
                int32_t file_kv_count = *kv_count_ - kLocalPerThread;
                int64_t x = file_kv_count * kSize4K - kDataBufferSize;
                if (x < 0)
                {
                    reverse_cache_dropped_ = true;
                    break;
                }

                if (pread64(read_fd_dio, cache_, kDataBufferSize, x) != kDataBufferSize)
                {
                    reverse_cache_dropped_ = true;
                    break;
                }

                reverse_cache_seqid_ = file_kv_count - 1;
                reverse_cache_pos_ = kDataBufferSize;
            }

            if (seqid != reverse_cache_seqid_)
            {
                reverse_cache_dropped_ = true;
                break;
            }

            if (reverse_cache_pos_ == 0)
            {
                int64_t x = (reverse_cache_seqid_ + 1) * kSize4K - kDataBufferSize;
                if (x < 0)
                {
                    reverse_cache_dropped_ = true;
                    break;
                }

                if (pread64(read_fd_dio, cache_, kDataBufferSize, x) != kDataBufferSize)
                {
                    reverse_cache_dropped_ = true;
                    break;
                }

                reverse_cache_pos_ = kDataBufferSize;
            }

#ifdef SLOW_DEBUG
            g_count_bio_reverse_cache++;
#endif
            reverse_cache_seqid_--;
            reverse_cache_pos_ -= kSize4K;
            return cache_ + reverse_cache_pos_;
        } while (0);

        return NULL;
    }

    char *find_DIO_slow(const int32_t seqid) const
    {
#ifdef SLOW_DEBUG
        g_count_dio_slow++;
#endif
        char *buf;
        posix_memalign((void **)&buf, kSize4K, kSize4K);
        pread64(read_fd_dio, buf, kSize4K, seqid * kSize4K);
        return buf;
    }

    void find_DIO_unsafe(const int32_t seqid, char *buf) const
    {
#ifdef SLOW_DEBUG
        g_count_unsafe_dio++;
#endif
        pread64(read_fd_dio, buf, kSize4K, seqid * kSize4K);
    }

    int32_t *hash_table_;
    char *buffer_;
    uint32_t *buffer_pos_;
    char *cache_;
    char *reverse_cache_;
    char *local_;
    int32_t *kv_count_;
    int read_fd_dio;
    int write_fd_;
    int index_fd_;
    int32_t cache_pos_;
    int32_t cache_seqid_;
    int32_t cache_len_;
    int32_t reverse_cache_pos_;
    int32_t reverse_cache_seqid_;
    bool is_init_;
    bool cache_dropped_;
    bool reverse_cache_dropped_;
};

class CompassDB
{
public:
    void thread_init(const char *dir, const int8_t thid)
    {
        if (!build_done_)
        {
            pthread_mutex_lock(&mutex_);
            if (!build_done_)
            {
                if (!FileExists(dir))
                    mkdir(dir, 0755);

                std::thread *threads[kThreadMax];
                for (int i = 0; i < kThreadMax; i++)
                    threads[i] = new std::thread(&__DB::build, db_ + i, dir, i);

                for (int i = 0; i < kThreadMax; i++)
                {
                    threads[i]->join();
                    delete threads[i];
                }

                build_done_ = true;
            }

            pthread_mutex_unlock(&mutex_);
        }

        get_count_[thid] = 0;
        set_count_[thid] = 0;
        db_[thid].init();
    }

    int Set(const KVString& key, const KVString& val, const int8_t thid)
    {
        int32_t left = set_count_[(thid + 7) & 7];
        int32_t right = set_count_[(thid + 1) & 7];
        int32_t now = set_count_[thid]++;
        if (left > 0 && right > 0 && now > left + 1024 && now > right + 1024)
            sched_yield();

        db_[thid].WAL(key.Buf(), val.Buf());
        return 0;
    }

    int Get(const KVString& key, KVString& val, const int8_t thid)
    {
        int32_t left = get_count_[(thid + 7) & 7];
        int32_t right = get_count_[(thid + 1) & 7];
        int32_t now = get_count_[thid]++;
        if (left > 0 && right > 0 && now > left + 1024 && now > right + 1024)
            sched_yield();

        return db_[thid].find(*((uint64_t *)key.Buf()), val);
    }

    static CompassDB* get_instance()
    {
        static CompassDB kInstance;
        return &kInstance;
    }

private:
    CompassDB():
        mutex_(PTHREAD_MUTEX_INITIALIZER),
        build_done_(false)
    {
        DEBUG("-DB CREATE-");
        memset(g_hash_table, -1, sizeof (g_hash_table));
        memset(g_kv_count, 0, sizeof (g_kv_count));
        posix_memalign((void **)&g_cache, kSize4K, kDataBufferSize * kThreadMax);
        posix_memalign((void **)&g_reverse_cache, kSize4K, kDataBufferSize * kThreadMax);
        memset(get_count_, 0, sizeof (get_count_));
        memset(set_count_, 0, sizeof (set_count_));

        for (int i = 0; i < kThreadMax; i++)
            g_local[i] = (char *)malloc(kLocalPerThread * kSize4K);
    }

    ~CompassDB()
    {
        DEBUG("local cache count: %d", (int32_t)g_count_local_cache);
        DEBUG("bio cache count: %d", (int32_t)g_count_bio_cache);
        DEBUG("bio reverse cache count: %d", (int32_t)g_count_bio_reverse_cache);
        DEBUG("dio slow count: %d", (int32_t)g_count_dio_slow);
        DEBUG("unsafe dio count: %d", (int32_t)g_count_unsafe_dio);
        DEBUG("-DB EXIT-");
    }

    pthread_mutex_t mutex_;
    int32_t get_count_[kThreadMax];
    int32_t set_count_[kThreadMax];
    __DB db_[kThreadMax];
    volatile bool build_done_;
};

static CompassDB *g_compass_db = CompassDB::get_instance();

class CompassKV : public KVIntf
{
public:
    CompassKV():
        thid_(-1)
    {
        DEBUG("Create KVIntf");
#ifdef SLOW_DEBUG
        set_count_ = 0;
        get_count_ = 0;
#endif
    }

    ~CompassKV()
    {
        DEBUG("Delete KVIntf");
    }

    bool Init(const char *dir, int id)
    {
        DEBUG("KVIntf.Init %s id=%d", dir, id);
#ifdef SLOW_DEBUG
        set_count_ = 0;
        get_count_ = 0;
#endif
        thid_ = id;
        g_compass_db->thread_init(dir, thid_);

        DEBUG("KVIntf.Init succ %s id=%d thid=%d", dir, id, thid_);
        return true;
    }

    void Close()
    {
        DEBUG("KVIntf.Close thid=%d", thid_);
#ifdef SLOW_DEBUG
        if (get_count_ == kAmountMaxPerThread)
        {
            DEBUG("KVIntf[%d] get count:%d, cost: %lldus", thid_, get_count_, TIMEVAL_VAL(get_st_, get_ed_));
        }

        if (set_count_ == kAmountMaxPerThread)
        {
            DEBUG("KVIntf[%d] set count:%d, cost: %lldus", thid_, set_count_, TIMEVAL_VAL(set_st_, set_ed_));
        }
#endif
        thid_ = -1;
    }

    int Set(KVString& key, KVString& val)
    {
#ifdef SLOW_DEBUG
        if (set_count_++ == 0)
        {
            TIMEVAL_GET(set_st_);
        }

        if (set_count_ == kAmountMaxPerThread)
        {
            TIMEVAL_GET(set_ed_);
        }
#endif

        return g_compass_db->Set(key, val, thid_);
    }

    int Get(KVString& key, KVString& val)
    {
#ifdef SLOW_DEBUG
        if (get_count_++ == 0)
        {
            TIMEVAL_GET(get_st_);
        }

        if (get_count_ == kAmountMaxPerThread)
        {
            TIMEVAL_GET(get_ed_);
        }
#endif

        return g_compass_db->Get(key, val, thid_);
    }

private:
    int8_t thid_;

#ifdef SLOW_DEBUG
    int32_t set_count_;
    int32_t get_count_;
    TIMEVAL_DEFINE(get_st_);
    TIMEVAL_DEFINE(set_st_);
    TIMEVAL_DEFINE(get_ed_);
    TIMEVAL_DEFINE(set_ed_);
#endif
};

std::shared_ptr<KVIntf> GetKVIntf()
{
    return std::make_shared<CompassKV>();
}

