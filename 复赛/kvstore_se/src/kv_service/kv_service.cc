#include <arpa/inet.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <sys/time.h>
#include <netinet/tcp.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <errno.h>
#include <poll.h>
#include <queue>
#include "kv_intf.h"
#include "kv_define.h"

#ifdef SLOW_DEBUG
  #include <atomic>
  static std::atomic<int32_t> g_count_new_buffer(0);
  static std::atomic<int32_t> g_count_write_seq_buffer(0);
  static std::atomic<int32_t> g_count_hit_seq_buffer(0);
  static std::atomic<int32_t> g_count_hit_reverse_buffer(0);
  static std::atomic<int32_t> g_count_get_from_network(0);
#endif

#define HASH_BIT    23
static const int32_t g_hash_len = (1 << HASH_BIT);
static const int32_t g_hash_mod = g_hash_len - 1;

class Lock
{
public:
    Lock(pthread_mutex_t& mutex): mutex_(&mutex) { pthread_mutex_lock(mutex_); }
    Lock(pthread_mutex_t *mutex): mutex_(mutex) { pthread_mutex_lock(mutex_); }
    ~Lock() { pthread_mutex_unlock(mutex_); }

private:
    pthread_mutex_t *mutex_;
};

static inline int __connect_server(const char *ip, unsigned short port)
{
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof (addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = inet_addr(ip);
    addr.sin_port = htons(port);

    int sockfd = socket(PF_INET, SOCK_STREAM, 0);
    if (connect(sockfd, (struct sockaddr *)&addr, sizeof (addr)) < 0)
    {
        ERROR("errno=%d, errmsg=%s", errno, strerror(errno));
        return -1;
    }

    return sockfd;
}

static inline int __connect_server_with_retry(const char *ip, unsigned short port)
{
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof (addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = inet_addr(ip);
    addr.sin_port = htons(port);

    for (int i = 0; i < kConnectRetryTimes; i++)
    {
        int sockfd = socket(PF_INET, SOCK_STREAM, 0);
        if (connect(sockfd, (struct sockaddr *)&addr, sizeof (addr)) < 0)
        {
            ERROR("retry-time(s): %d, errno=%d, errmsg=%s", i, errno, strerror(errno));
            usleep(1000 * 1000);
            continue;
        }

        return sockfd;
    }

    ERROR("Cannot connect to server, already retry %d times", kConnectRetryTimes);

    return -1;
}

static int32_t g_hash_table[kThreadMax][g_hash_len];
static uint64_t g_key[kThreadMax][kAmountMaxPerThread];
static int32_t g_next[kThreadMax][kAmountMaxPerThread];
static int32_t g_kv_count[kThreadMax];
static int g_fd0;

struct __work_param
{
    char *buf;
    int32_t seqid;
    int32_t num;
    uint8_t is_value;
    uint8_t fileid;
    uint8_t callback_type;
    uint8_t u8;
};

static std::queue<__work_param> work_queue;

#define BLOCK_DISABLE    -1
#define BLOCK_READY      0
#define BLOCK_READING    1

static volatile bool is_stop = false;
static pthread_mutex_t work_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t work_cond = PTHREAD_COND_INITIALIZER;
static pthread_mutex_t index_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t index_cond = PTHREAD_COND_INITIALIZER;
static int index_task_num;
static pthread_mutex_t seq_done_mutex[kThreadMax][kCacheBlockNum];
static pthread_cond_t seq_done_cond[kThreadMax][kCacheBlockNum];
static pthread_mutex_t reverse_done_mutex[kThreadMax][kCacheBlockNum];
static pthread_cond_t reverse_done_cond[kThreadMax][kCacheBlockNum];
static int seq_block_state[kThreadMax][kCacheBlockNum];// -1 disable | 0 ready | 1 reading from network
static int reverse_block_state[kThreadMax][kCacheBlockNum];
static pthread_mutex_t get_mutex[kThreadMax];
static pthread_cond_t get_cond[kThreadMax];
static bool get_done[kThreadMax];

#define INDEX_CB        0
#define SEQ_CB          1
#define REVERSE_CB      2
#define RANDOM_GET_CB   3

static inline void index_callback(int8_t fileid)
{
    int32_t num = g_kv_count[fileid];
    uint64_t *key = g_key[fileid];
    int32_t *next = g_next[fileid];
    int32_t *hash_table = g_hash_table[fileid];
    for (int i = 0; i < num; i++)
    {
        int32_t hash = (key[i] & g_hash_mod);
        next[i] = hash_table[hash];
        hash_table[hash] = i;
    }
    pthread_mutex_lock(&index_mutex);
    if (--index_task_num == 0)
    {
        pthread_mutex_unlock(&index_mutex);
        pthread_cond_signal(&index_cond);
    }
    else
        pthread_mutex_unlock(&index_mutex);
}

static inline void seq_callback(uint8_t block_id, uint8_t fileid)
{
    pthread_mutex_lock(&seq_done_mutex[fileid][block_id]);
    seq_block_state[fileid][block_id] = BLOCK_READY;
    pthread_mutex_unlock(&seq_done_mutex[fileid][block_id]);
    pthread_cond_signal(&seq_done_cond[fileid][block_id]);
}

static inline void reverse_callback(uint8_t block_id, uint8_t fileid)
{
    pthread_mutex_lock(&reverse_done_mutex[fileid][block_id]);
    reverse_block_state[fileid][block_id] = BLOCK_READY;
    pthread_mutex_unlock(&reverse_done_mutex[fileid][block_id]);
    pthread_cond_signal(&reverse_done_cond[fileid][block_id]);
}

static inline void random_get_callback(uint8_t fileid)
{
    pthread_mutex_lock(&get_mutex[fileid]);
    get_done[fileid] = true;
    pthread_mutex_unlock(&get_mutex[fileid]);
    pthread_cond_signal(&get_cond[fileid]);
}

struct __get_routine_param
{
    int fd;
    uint8_t thid;
};

static void *__get_routine(void *arg)
{
    __get_routine_param *thread_param = (__get_routine_param *)arg;
    int fd = thread_param->fd;
    //uint8_t thid = thread_param->thid;
    struct pollfd poll_fd;
    poll_fd.fd = fd;

    int ret;
    uint64_t info;

    __work_param param;
    while (!is_stop)
    {
        pthread_mutex_lock(&work_mutex);
        while (!is_stop && work_queue.empty())
            pthread_cond_wait(&work_cond, &work_mutex);

        if (is_stop)
        {
            pthread_mutex_unlock(&work_mutex);
            break;
        }
        else
        {
            param = work_queue.front();
            work_queue.pop();
            pthread_mutex_unlock(&work_mutex);
        }

        while (!is_stop)
        {
            TIMEVAL_DEFINEGET(tv1);

            //[seqid|num|fileid|iorv]
            info = param.seqid;
            info <<= 22;
            info |= param.num;
            info <<= 4;
            info |= param.fileid;
            info <<= 1;
            info |= param.is_value;
            ret = __write_with_poll(&poll_fd, &info, kSize8B);
            if (ret < 0)
            {
                ERROR("errno=%d, errmsg=%s", errno, strerror(errno));
                return NULL;
            }
            else if (ret == 0)
            {
                ERROR("connection is closed");
                return NULL;
            }

            int rlen = param.num * (param.is_value ? kSize4K : kSize8B);
            ret = __read_with_poll(&poll_fd, param.buf, rlen);
            if (ret < 0)
            {
                ERROR("errno=%d, errmsg=%s", errno, strerror(errno));
                return NULL;
            }
            else if (ret == 0)
            {
                ERROR("connection is closed");
                return NULL;
            }

            if (param.num > 1 && param.seqid < 12800)
            {
                TIMEVAL_DEFINEGET(tv2);
                DEBUG("update[%d] from server, is_value(%d) seqid[%d] num[%d], cost: %lldus", param.fileid, param.is_value, param.seqid, param.num, TIMEVAL_VAL(tv1, tv2));
            }

            switch (param.callback_type)
            {
            case INDEX_CB://index
                index_callback(param.u8);
                break;
            case SEQ_CB://seq
                seq_callback(param.u8, param.fileid);
                break;
            case REVERSE_CB://reverse
                reverse_callback(param.u8, param.fileid);
                break;
            case RANDOM_GET_CB://random get
                random_get_callback(param.u8);
                break;
            }

            pthread_mutex_lock(&work_mutex);
            if (work_queue.empty())
            {
                pthread_mutex_unlock(&work_mutex);
                break;
            }
            else
            {
                param = work_queue.front();
                work_queue.pop();
                pthread_mutex_unlock(&work_mutex);
            }
        }
    }

    return NULL;
}

class SequenceCacheTable
{
public:
    SequenceCacheTable():
        cache_(NULL),
        cache_len_(0),
        cache_st_seqid_(0),
        cache_ed_seqid_(0),
        now_block_id_(0),
        thid_(0)
    {
        cache_ = (char *)malloc(kCacheSize);
    }

    ~SequenceCacheTable()
    {
        free(cache_);
    }

    void set_thid(const uint8_t thid)
    {
        thid_ = thid;
        for (int i = 0; i < kCacheBlockNum; i++)
            seq_block_state[thid][i] = BLOCK_READY;
    }

    bool append(int32_t seqid, const char *buf)
    {
        if (cache_len_ >= kCacheSize)
            return false;

        if (seqid != cache_ed_seqid_)
            return false;

        memcpy(cache_ + cache_len_, buf, kSize4K);
        cache_ed_seqid_++;
        cache_len_ += kSize4K;
        return true;
    }

    const char *find(int32_t seqid)
    {
        if (seqid < cache_ed_seqid_ && seqid >= cache_st_seqid_ + kKVNumberPerBlock && seqid < cache_st_seqid_ + kKVNumberPerBlock * 2)//in second block
        {
            int8_t second_block_id = (now_block_id_ + 1) % kCacheBlockNum;
            int32_t kv_count = g_kv_count[thid_];

            seq_block_state[thid_][now_block_id_] = BLOCK_DISABLE;
            if (cache_ed_seqid_ > cache_st_seqid_ && cache_ed_seqid_ - cache_st_seqid_ == kCacheKVNumber && cache_ed_seqid_ < kv_count)
            {
                int32_t new_ed_seqid = __MIN(cache_ed_seqid_ + kKVNumberPerBlock, kv_count);
                int32_t n = new_ed_seqid - cache_ed_seqid_;

                if (n > 0)
                {
                    seq_block_state[thid_][now_block_id_] = BLOCK_READING;
                    pthread_mutex_lock(&work_mutex);
                    work_queue.push({
                        cache_ + now_block_id_ * kCacheSizePerBlock,
                        cache_ed_seqid_,
                        n,
                        1,
                        thid_,
                        SEQ_CB,
                        now_block_id_
                    });
                    pthread_mutex_unlock(&work_mutex);
                    pthread_cond_signal(&work_cond);
                }

                cache_ed_seqid_ = new_ed_seqid;
            }

            cache_st_seqid_ += kKVNumberPerBlock;
            now_block_id_ = second_block_id;
        }

        if (!(seqid >= cache_st_seqid_ && seqid < cache_ed_seqid_))
            return NULL;

        int32_t forward_nblock = (seqid - cache_st_seqid_) / kKVNumberPerBlock;
        int32_t block_st_seqid = cache_st_seqid_ + forward_nblock * kKVNumberPerBlock;
        int32_t block_id = (now_block_id_ + forward_nblock) % kCacheBlockNum;

        if (seq_block_state[thid_][block_id] == BLOCK_READING)
        {
            pthread_mutex_lock(&seq_done_mutex[thid_][block_id]);
            while (seq_block_state[thid_][block_id] == BLOCK_READING)
                pthread_cond_wait(&seq_done_cond[thid_][block_id], &seq_done_mutex[thid_][block_id]);

            pthread_mutex_unlock(&seq_done_mutex[thid_][block_id]);
        }

        if (seq_block_state[thid_][block_id] == BLOCK_DISABLE)
            return NULL;

        return cache_ + block_id * kCacheSizePerBlock + (seqid - block_st_seqid) * kSize4K;
    }

private:
    char *cache_;
    int32_t cache_len_;
    int32_t cache_st_seqid_;
    int32_t cache_ed_seqid_;
    uint8_t now_block_id_;
    uint8_t thid_;
};

class ReverseCacheTable
{
public:
    ReverseCacheTable():
        cache_(NULL),
        cache_st_seqid_(-1),
        cache_ed_seqid_(-1),
        now_block_id_(kCacheBlockNum - 1),
        thid_(0)
    {
        cache_ = (char *)malloc(kCacheSize);
    }

    ~ReverseCacheTable()
    {
        free(cache_);
    }

    void set_thid(const uint8_t thid)
    {
        thid_ = thid;
        for (int i = 0; i < kCacheBlockNum; i++)
            reverse_block_state[thid][i] = BLOCK_DISABLE;
    }

    const char *find(int32_t seqid)
    {
        int32_t kv_count = g_kv_count[thid_];

        if (cache_st_seqid_ < 0 && cache_ed_seqid_ < 0)//new request
        {
            if (kv_count > 0 && seqid >= 0 && seqid >= kv_count - kCacheKVNumber && seqid < kv_count)//in reverse all block range
            {
                cache_ed_seqid_ = kv_count;
                cache_st_seqid_ = __MAX(kv_count - kCacheKVNumber, 0);

                int32_t ed = kv_count;
                for (int i = 0; i < kCacheBlockNum; i++)
                {
                    int block_id = kCacheBlockNum - 1 - i;
                    int32_t st = __MAX(ed - kKVNumberPerBlock, 0);
                    int32_t n = ed - st;

                    if (n > 0)
                    {
                        reverse_block_state[thid_][block_id] = BLOCK_READING;
                        pthread_mutex_lock(&work_mutex);
                        work_queue.push({
                            cache_ + block_id * kCacheSizePerBlock + (kKVNumberPerBlock - n) * kSize4K,
                            st,
                            n,
                            1,
                            thid_,
                            REVERSE_CB,
                            (uint8_t)block_id
                        });
                        pthread_mutex_unlock(&work_mutex);
                        pthread_cond_signal(&work_cond);
                    }
                    else
                        break;

                    ed = st;
                }
            }
            else
                return NULL;
        }
        else if (seqid >= 0 && seqid >= cache_ed_seqid_ - kKVNumberPerBlock * 2 && seqid < cache_ed_seqid_ - kKVNumberPerBlock)//in second block
        {
            int8_t second_block_id = (now_block_id_ + kCacheBlockNum - 1) % kCacheBlockNum;

            reverse_block_state[thid_][now_block_id_] = BLOCK_DISABLE;
            if (cache_ed_seqid_ > cache_st_seqid_ && cache_ed_seqid_ - cache_st_seqid_ == kCacheKVNumber && cache_st_seqid_ >= 0)
            {
                int32_t new_st_seqid = __MAX(cache_st_seqid_ - kKVNumberPerBlock, 0);
                int32_t n = cache_st_seqid_ - new_st_seqid;

                if (n > 0)
                {
                    reverse_block_state[thid_][now_block_id_] = BLOCK_READING;
                    pthread_mutex_lock(&work_mutex);
                    work_queue.push({
                        cache_ + now_block_id_ * kCacheSizePerBlock + (kKVNumberPerBlock - n) * kSize4K,
                        new_st_seqid,
                        n,
                        1,
                        thid_,
                        REVERSE_CB,
                        now_block_id_
                    });
                    pthread_mutex_unlock(&work_mutex);
                    pthread_cond_signal(&work_cond);
                }

                cache_st_seqid_ = new_st_seqid;
            }

            cache_ed_seqid_ -= kKVNumberPerBlock;
            now_block_id_ = second_block_id;
        }

        if (!(seqid >= cache_st_seqid_ && seqid < cache_ed_seqid_))
            return NULL;

        int32_t backward_nblock = (cache_ed_seqid_ - 1 - seqid) / kKVNumberPerBlock;
        int32_t block_ed_seqid = cache_ed_seqid_ - backward_nblock * kKVNumberPerBlock;
        int32_t block_id = (now_block_id_ + kCacheBlockNum - backward_nblock) % kCacheBlockNum;

        if (reverse_block_state[thid_][block_id] == BLOCK_READING)
        {
            pthread_mutex_lock(&reverse_done_mutex[thid_][block_id]);
            while (reverse_block_state[thid_][block_id] == BLOCK_READING)
                pthread_cond_wait(&reverse_done_cond[thid_][block_id], &reverse_done_mutex[thid_][block_id]);

            pthread_mutex_unlock(&reverse_done_mutex[thid_][block_id]);
        }

        if (reverse_block_state[thid_][block_id] == BLOCK_DISABLE)
            return NULL;

        return cache_ + block_id * kCacheSizePerBlock + (seqid - (block_ed_seqid - kKVNumberPerBlock)) * kSize4K;
    }

private:
    char *cache_;
    int32_t cache_st_seqid_;
    int32_t cache_ed_seqid_;
    uint8_t now_block_id_;
    uint8_t thid_;
};

class IndexTable
{
public:
    IndexTable():
        hash_table_(NULL),
        key_(NULL),
        next_(NULL),
        index_arr_size_(NULL),
        thid_(0)
    {
    }

    void set_thid(const uint8_t thid)
    {
        thid_ = thid;
        hash_table_ = g_hash_table[thid_];
        key_ = g_key[thid_];
        next_ = g_next[thid_];
        index_arr_size_ = g_kv_count + thid_;
        *index_arr_size_ = 0;
        memset(hash_table_, -1, sizeof (int32_t) * g_hash_len);
    }

    int32_t append(uint64_t key_i64) const
    {
        int32_t seqid = (*index_arr_size_)++;
        int32_t hash = (key_i64 & g_hash_mod);

        key_[seqid] = key_i64;
        next_[seqid] = hash_table_[hash];
        hash_table_[hash] = seqid;

        return seqid;
    }

    int32_t find(const uint64_t key_i64) const
    {
        int32_t idx = hash_table_[key_i64 & g_hash_mod];
        while (idx >= 0)
        {
            if (key_[idx] == key_i64)
                break;

            idx = next_[idx];
        }

        return idx;
    }

private:
    int32_t *hash_table_;
    uint64_t *key_;
    int32_t *next_;
    int32_t *index_arr_size_;
    uint8_t thid_;
};

class __GET
{
public:
    static __GET *get_instance()
    {
        static __GET kInstance;
        return &kInstance;
    }

private:
    __GET();
};

class CompassDB
{
public:
    CompassDB():
        mutex_(PTHREAD_MUTEX_INITIALIZER),
        which_cache_(0),
        is_ready_(false)
    {
        DEBUG("-DB CREATE-");
        DEBUG("kDataBufferSize %d", kDataBufferSize);
        DEBUG("kIndexBufferSize %d", kIndexBufferSize);
        DEBUG("kTCPDataPackage %d", kTCPDataPackage);
        DEBUG("kCacheBlockNum %d", kCacheBlockNum);

        memset(thread_fileid_, -1, sizeof (thread_fileid_));
        memset(seq_cache_set_close_, 0, sizeof (seq_cache_set_close_));
        memset(get_threads_, -1, sizeof (get_threads_));
        memset(cache_fd_, -1, sizeof (cache_fd_));
        for (int i = 0; i < kThreadMax; i++)
        {
            set_fd_[i].fd = -1;
            index_table_[i].set_thid(i);
            seq_cache_table_[i].set_thid(i);
            reverse_cache_table_[i].set_thid(i);
        }

        for (int i = 0; i < kThreadMax; i++)
        {
            pthread_mutex_init(&get_mutex[i], NULL);
            pthread_cond_init(&get_cond[i], NULL);
            for (int j = 0; j < kCacheBlockNum; j++)
            {
                pthread_mutex_init(&seq_done_mutex[i][j], NULL);
                pthread_mutex_init(&reverse_done_mutex[i][j], NULL);
                pthread_cond_init(&seq_done_cond[i][j], NULL);
                pthread_cond_init(&reverse_done_cond[i][j], NULL);
            }
        }
    }

    ~CompassDB()
    {
        pthread_mutex_lock(&work_mutex);
        is_stop = true;
        pthread_mutex_unlock(&work_mutex);
        pthread_cond_broadcast(&work_cond);

        for (int i = 0; i < kThreadMax; i++)
        {
            if (get_threads_[i] >= 0)
                pthread_join(get_threads_[i], NULL);
        }

        for (int i = 0; i < kThreadMax; i++)
        {
            if (cache_fd_[i] >= 0)
                close(cache_fd_[i]);
        }

        for (int i = 0; i < kThreadMax; i++)
        {
            if (set_fd_[i].fd >= 0)
                close(set_fd_[i].fd);
        }

        for (int i = 0; i < kThreadMax; i++)
        {
            pthread_mutex_destroy(&get_mutex[i]);
            pthread_cond_destroy(&get_cond[i]);
            for (int j = 0; j < kCacheBlockNum; j++)
            {
                pthread_mutex_destroy(&seq_done_mutex[i][j]);
                pthread_mutex_destroy(&reverse_done_mutex[i][j]);
                pthread_cond_destroy(&seq_done_cond[i][j]);
                pthread_cond_destroy(&reverse_done_cond[i][j]);
            }
        }

        DEBUG("key[0] count: %d", g_kv_count[0]);
        DEBUG("new buffer count: %d", (int32_t)g_count_new_buffer);
        DEBUG("write sequence buffer count: %d", (int32_t)g_count_write_seq_buffer);
        DEBUG("hit sequence buffer count: %d", (int32_t)g_count_hit_seq_buffer);
        DEBUG("hit reverse buffer count: %d", (int32_t)g_count_hit_reverse_buffer);
        DEBUG("get from network count: %d", (int32_t)g_count_get_from_network);
        DEBUG("-DB EXIT-");
    }

    void thread_init(const char *ip, const int8_t thid)
    {
        if (!is_ready_)
        {
            Lock lock(mutex_);
            if (!is_ready_)
            {
                ip_ = ip;
                g_fd0 = __connect_server_with_retry(ip, kPortStartForSet);
                if (g_fd0 < 0)
                {
                    ERROR("errno=%d, errmsg=%s", errno, strerror(errno));
                    return;
                }

                DEBUG("first connect fd=%d", g_fd0);
                if (__set_fd_nonblock(g_fd0) < 0)
                {
                    ERROR("errno=%d, errmsg=%s", errno, strerror(errno));
                    close(g_fd0);
                    return;
                }

                setsockopt(g_fd0, IPPROTO_TCP, TCP_NODELAY, &kOn, sizeof (kOn));
                set_fd_[0].fd = g_fd0;

                TIMEVAL_DEFINEGET(tv1);
                int ret = __read_with_poll(&set_fd_[0], g_kv_count, kSize4B * kThreadMax);
                if (ret < 0)
                {
                    ERROR("errno=%d, errmsg=%s", errno, strerror(errno));
                    return;
                }
                else if (ret == 0)
                {
                    ERROR("connection is closed");
                    return;
                }
                TIMEVAL_DEFINEGET(tv2);

                index_task_num = 0;
                for (uint8_t fileid = 0; fileid < kThreadMax; fileid++)
                {
                    int32_t num = g_kv_count[fileid];
                    if (num > 0)
                    {
                        index_task_num++;
                        pthread_mutex_lock(&work_mutex);
                        work_queue.push({
                            (char *)(g_key[fileid]),
                            0,
                            num,
                            0,
                            fileid,
                            INDEX_CB,
                            fileid
                        });
                        pthread_mutex_unlock(&work_mutex);
                        pthread_cond_signal(&work_cond);

                        DEBUG("index[%d] count[%d] prepare to restore", fileid, num);
                    }
                }

                get_init();

                if (index_task_num > 0)
                {
                    pthread_mutex_lock(&index_mutex);
                    while (index_task_num > 0)
                        pthread_cond_wait(&index_cond, &index_mutex);

                    pthread_mutex_unlock(&index_mutex);
                }

                pthread_mutex_lock(&work_mutex);
                is_stop = true;
                pthread_mutex_unlock(&work_mutex);
                pthread_cond_broadcast(&work_cond);

                for (int i = 0; i < kThreadMax; i++)
                {
                    if (get_threads_[i] >= 0)
                    {
                        pthread_join(get_threads_[i], NULL);
                        get_threads_[i] = -1;
                    }
                }

                for (int i = 0; i < kThreadMax; i++)
                {
                    if (cache_fd_[i] >= 0)
                    {
                        close(cache_fd_[i]);
                        cache_fd_[i] = -1;
                    }
                }

                TIMEVAL_DEFINEGET(tv3);
                DEBUG("IndexTable restore from server cost: %lldus %lldus", TIMEVAL_VAL(tv1, tv2), TIMEVAL_VAL(tv2, tv3));

                set_init();

                is_ready_ = true;
            }
        }

        set_count_[thid] = 0;
        thread_fileid_[thid] = -1;
        which_cache_ = 0;
    }

    int Set(const KVString& key, const KVString& val, const int8_t thid)
    {
        if (set_count_[thid]++ == 0)
            usleep(thid * 8000);

        const char *key_buf = key.Buf();
        const char *val_buf = val.Buf();

        struct iovec iov[2];
        iov[0].iov_base = const_cast<char *>(val_buf);
        iov[0].iov_len = kSize4K;
        iov[1].iov_base = const_cast<char *>(key_buf);
        iov[1].iov_len = kSize8B;

        //setsockopt(set_fd_[thid].fd, IPPROTO_TCP, TCP_QUICKACK, &kOn, sizeof (kOn));
        __writev_with_poll(&set_fd_[thid], iov, 2);

        int32_t seqid = index_table_[thid].append(*((uint64_t *)key_buf));
        if (!seq_cache_set_close_[thid] && !seq_cache_table_[thid].append(seqid, val_buf))
            seq_cache_set_close_[thid] = true;

        bool flag;
        //setsockopt(set_fd_[thid].fd, IPPROTO_TCP, TCP_QUICKACK, &kOn, sizeof (kOn));
        __read_with_poll(&set_fd_[thid], &flag, kSize1B);

        return 0;
    }

    int Get(const KVString& key, KVString& val, const int8_t thid)
    {
        static __GET *p = __GET::get_instance();
        (void)p;

        int8_t fileid = thread_fileid_[thid];
        int32_t seqid = -1;
        uint64_t key_i64 = *((uint64_t *)key.Buf());
        bool already_4k = (val.Size() == kSize4K);

        if (fileid < 0)
        {
            for (int i = 0; i < kThreadMax; i++)
            {
                seqid = index_table_[i].find(key_i64);
                if (seqid >= 0)
                {
                    fileid = i;
                    thread_fileid_[thid] = i;
                    break;
                }
            }
        }

        if (fileid < 0)
            return -1;

        if (seqid < 0)
        {
            seqid = index_table_[fileid].find(key_i64);
            if (seqid < 0)
                return -1;
        }

        for (int z = 0; z < 2; z++)
        {
            if (which_cache_ == 0)
            {
                const char *buf = seq_cache_table_[fileid].find(seqid);
                if (buf)
                {
#ifdef SLOW_DEBUG
                    g_count_hit_seq_buffer++;
#endif
                    if (already_4k)
                        memcpy(const_cast<char *>(val.Buf()), buf, kSize4K);
                    else
                        val = KVString(buf, kSize4K);

                    return 0;
                }
            }
            else
            {
                const char *buf = reverse_cache_table_[fileid].find(seqid);
                if (buf)
                {
#ifdef SLOW_DEBUG
                    g_count_hit_reverse_buffer++;
#endif
                    if (already_4k)
                        memcpy(const_cast<char *>(val.Buf()), buf, kSize4K);
                    else
                        val = KVString(buf, kSize4K);

                    return 0;
                }
            }

            which_cache_ = 1 - which_cache_;
        }

#ifdef SLOW_DEBUG
        g_count_get_from_network++;
#endif

        char *buf;

        if (already_4k)
            buf = const_cast<char *>(val.Buf());
        else
        {
#ifdef SLOW_DEBUG
            g_count_new_buffer++;
#endif
            buf = new char[kSize4K];
        }

        get_done[fileid] = false;
        pthread_mutex_lock(&work_mutex);
        work_queue.push({buf, seqid, 1, 1, (uint8_t)fileid, RANDOM_GET_CB, (uint8_t)fileid});
        pthread_mutex_unlock(&work_mutex);
        pthread_cond_signal(&work_cond);

        if (!get_done[fileid])
        {
            pthread_mutex_lock(&get_mutex[fileid]);
            while (!get_done[fileid])
                pthread_cond_wait(&get_cond[fileid], &get_mutex[fileid]);

            pthread_mutex_unlock(&get_mutex[fileid]);
        }

        if (!already_4k)
            val.Reset(buf, kSize4K);

        return 0;
    }

    void set_init()
    {
        DEBUG("set init");
        //set[0] is already connect
        for (int i = 0; i < kThreadMax; i++)
        {
            int fd;
            if (i == 0)
                fd = g_fd0;
            else
            {
                fd = __connect_server(ip_, kPortStartForSet + i);
                if (__set_fd_nonblock(fd) < 0)
                {
                    ERROR("errno=%d, errmsg=%s", errno, strerror(errno));
                    close(fd);
                    return;
                }

                setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &kOn, sizeof (kOn));
                set_fd_[i].fd = fd;
            }
        }
    }

    void get_init()
    {
        DEBUG("get init");
        for (int i = 0; i < kThreadMax; i++)
        {
            int fd = __connect_server(ip_, kPortStartForGet + i);
            if (__set_fd_nonblock(fd) < 0)
            {
                ERROR("errno=%d, errmsg=%s", errno, strerror(errno));
                close(fd);
                return;
            }

            setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &kOn, sizeof (kOn));
            cache_fd_[i] = fd;
        }

        is_stop = false;
        for (uint8_t i = 0; i < kThreadMax; i++)
        {
            get_thread_param_[i] = {cache_fd_[i], i};
            pthread_create(&get_threads_[i], NULL, __get_routine, &get_thread_param_[i]);
        }
    }

private:
    const char *ip_;
    struct pollfd set_fd_[kThreadMax];
    int cache_fd_[kThreadMax];
    int32_t set_count_[kThreadMax];
    IndexTable index_table_[kThreadMax];
    SequenceCacheTable seq_cache_table_[kThreadMax];
    ReverseCacheTable reverse_cache_table_[kThreadMax];
    int8_t thread_fileid_[kThreadMax];
    bool seq_cache_set_close_[kThreadMax];
    pthread_t get_threads_[kThreadMax];
    __get_routine_param get_thread_param_[kThreadMax];
    pthread_mutex_t mutex_;
    int8_t which_cache_;
    volatile bool is_ready_;
};

static CompassDB g_compass_db;

__GET::__GET()
{
    g_compass_db.get_init();
}

class CompassKV : public KVIntf
{
public:
    CompassKV():
        thid_(-1)
    {
    }

    bool Init(const char *host, int id)
    {
        thid_ = id;
        g_compass_db.thread_init(host + 6, thid_);
        return true;
    }

    void Close()
    {
        thid_ = -1;
    }

    int Set(KVString& key, KVString& val)
    {
        return g_compass_db.Set(key, val, thid_);
    }

    int Get(KVString& key, KVString& val)
    {
        return g_compass_db.Get(key, val, thid_);
    }

private:
    int8_t thid_;
};

std::shared_ptr<KVIntf> GetKVIntf()
{
    return std::make_shared<CompassKV>();
}

