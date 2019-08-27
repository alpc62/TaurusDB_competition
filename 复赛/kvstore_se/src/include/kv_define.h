#ifndef __KV_DEFINE__
#define __KV_DEFINE__

#ifdef SLOW_DEBUG
  #include <stdio.h>
  #include <sys/time.h>
  #define DEBUG(format,...)     printf("[DEBUG] " format "\n", ##__VA_ARGS__)
  #define ERROR(format,...)     printf("[ERROR] %s:%s[%d] " format "\n", __FILE__, __FUNCTION__, __LINE__, ##__VA_ARGS__)
  #define FLUSH(x)              fflush(x);
  #define TIMEVAL_DEFINE(x)     struct timeval (x)
  #define TIMEVAL_GET(x)        gettimeofday(&(x), NULL)
  #define TIMEVAL_DEFINEGET(x)  struct timeval (x); gettimeofday(&(x), NULL)
  #define TIMEVAL_VAL(tv1, tv2) (((long long)(tv2).tv_sec - (tv1).tv_sec) * 1000000 + (tv2).tv_usec - (tv1).tv_usec)
#else
  #define DEBUG(format,...)
  #define ERROR(format,...)
  #define FLUSH(x)
  #define TIMEVAL_DEFINE(x)
  #define TIMEVAL_GET(x)
  #define TIMEVAL_DEFINEGET(x)
  #define TIMEVAL_VAL(tv1, tv2)
#endif

#define kAmountMaxPerThread (400 * 10000)
#define kThreadMax          16
#define kSize1B             1
#define kSize4B             4
#define kSize8B             8
#define kSize4K             4096LL
#define kPortStartForSet    9500
#define kPortStartForGet    9516
#define kDataBufferSize     (512 * 1024)
#define kIndexBufferSize    (512 * 1024)
#define kConnectRetryTimes  32
#define kTCPDataPackage     (4 * 1024)
#define kCacheSizePerBlock  (10 * 1024 * 1024)
#define kCacheBlockNum      3

#define kCacheSize          (kCacheSizePerBlock * kCacheBlockNum)
#define kKVNumberPerBlock   (kCacheSizePerBlock / 4096)
#define kCacheKVNumber      (kKVNumberPerBlock * kCacheBlockNum)
#define kIndexBufferNum     (kIndexBufferSize / 8)
#define kDataBufferNum      (kDataBufferSize / 4096)

static const int kOn = 1;
static const int kOff = 0;

#define __MIN(x, y) (((x) < (y)) ? (x) : (y))
#define __MAX(x, y) (((x) > (y)) ? (x) : (y))

#include <unistd.h>
#include <fcntl.h>
#include <poll.h>
#include <errno.h>
static inline int __set_fd_nonblock(const int fd)
{
    int flags = fcntl(fd, F_GETFL);

    if (flags >= 0)
        flags = fcntl(fd, F_SETFL, flags | O_NONBLOCK);

    return flags;
}

static inline int __write_with_poll(struct pollfd *poll_fd, const void *buf, int buflen)
{
    int ret;
    int nready;

    poll_fd->events = POLLOUT;
    while (buflen > 0)
    {
        ret = write(poll_fd->fd, buf, buflen);
        if (ret > 0)
        {
            buf = (char *)buf + ret;
            buflen -= ret;
        }
        else if (ret < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))
        {
            nready = poll(poll_fd, 1, -1);
            if (nready <= 0)
                return nready;

            continue;
        }
        else
            return ret;
    }

    return 1;//succ
}

static inline int __read_with_poll(struct pollfd *poll_fd, void *buf, int buflen)
{
    int ret;
    int nready;

    poll_fd->events = POLLIN;
    while (buflen > 0)
    {
        ret = read(poll_fd->fd, buf, buflen);
        if (ret > 0)
        {
            buf = (char *)buf + ret;
            buflen -= ret;
        }
        else if (ret < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))
        {
            nready = poll(poll_fd, 1, -1);
            if (nready <= 0)
                return nready;

            continue;
        }
        else
            return ret;
    }

    return 1;//succ
}

static inline int __writev_with_poll(struct pollfd *poll_fd, struct iovec *iov, int iov_cnt)
{
    int ret;
    int nready;

    poll_fd->events = POLLOUT;
    while (iov_cnt > 0)
    {
        ret = writev(poll_fd->fd, iov, iov_cnt);
        if (ret > 0)
        {
            do
            {
                if ((size_t)ret < iov[0].iov_len)
                {
                    iov[0].iov_base = (char *)iov[0].iov_base + ret;
                    iov[0].iov_len -= ret;
                    ret = 0;
                }
                else
                {
                    iov_cnt--;
                    ret -= iov[0].iov_len;
                    iov++;
                }
            } while (ret > 0);
        }
        else if (ret < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))
        {
            nready = poll(poll_fd, 1, -1);
            if (nready <= 0)
                return nready;

            continue;
        }
        else
            return ret;
    }

    return 1;//succ
}

static inline int __readv_with_poll(struct pollfd *poll_fd, struct iovec *iov, int iov_cnt)
{
    int ret;
    int nready;

    poll_fd->events = POLLIN;
    while (iov_cnt > 0)
    {
        ret = readv(poll_fd->fd, iov, iov_cnt);
        if (ret > 0)
        {
            do
            {
                if ((size_t)ret < iov[0].iov_len)
                {
                    iov[0].iov_base = (char *)iov[0].iov_base + ret;
                    iov[0].iov_len -= ret;
                    ret = 0;
                }
                else
                {
                    iov_cnt--;
                    ret -= iov[0].iov_len;
                    iov++;
                }
            } while (ret > 0);
        }
        else if (ret < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))
        {
            nready = poll(poll_fd, 1, -1);
            if (nready <= 0)
                return nready;

            continue;
        }
        else
            return ret;
    }

    return 1;//succ
}

#endif

