#define _GNU_SOURCE
#include <arpa/inet.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/uio.h>
#include <sys/time.h>
#include <netinet/tcp.h>
#include <stdio.h>
#include <dirent.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <stdint.h>
#include <pthread.h>
#include <poll.h>
#include "kv_define.h"

static inline int64_t GetFileLength(const char *path)
{
    struct stat stat_buf;

    return stat(path, &stat_buf) == 0 ? stat_buf.st_size : -1;
}

static inline int __server_listen(const char *ip, unsigned short port, int conn_max)
{
    int listenfd;
    struct sockaddr_in addr;

    listenfd = socket(AF_INET, SOCK_STREAM, 0);
    if (listenfd < 0)
    {
        ERROR("errno=%d, errmsg=%s", errno, strerror(errno));
        return -1;
    }

    if (setsockopt(listenfd, SOL_SOCKET, SO_REUSEADDR, &kOn, sizeof (kOn)) < 0)
    {
        ERROR("errno=%d, errmsg=%s", errno, strerror(errno));
        return -1;
    }

    memset(&addr, 0, sizeof (addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = inet_addr(ip);
    addr.sin_port = htons(port);
    if (bind(listenfd, (struct sockaddr*)&addr, sizeof(addr)) < 0)
    {
        ERROR("errno=%d, errmsg=%s", errno, strerror(errno));
        return -1;
    }

    if (listen(listenfd, 2 * conn_max) < 0)
    {
        ERROR("errno=%d, errmsg=%s", errno, strerror(errno));
        return -1;
    }

    return listenfd;
}

/* memory */
static int32_t memory_kv_num[kThreadMax];
static uint8_t kDigit[kThreadMax];

/* counter */
static int32_t *mmap_counter_arr;

/* index */
static int index_read_fd_bio[kThreadMax];
static int index_write_fd_dio[kThreadMax];
static char *index_buffer[kThreadMax];
static uint32_t index_buffer_num[kThreadMax];

/* data */
static int data_read_fd_bio[kThreadMax];
static int data_write_fd_dio[kThreadMax];
static char *data_buffer[kThreadMax];
static uint32_t data_buffer_num[kThreadMax];

/* server */
static const char *server_ip;
static pthread_t server_set_threads[kThreadMax];
static pthread_t server_get_threads[kThreadMax];

static inline char *__index_WAL_prepare(const uint8_t thid)
{
    if (index_buffer_num[thid] < kIndexBufferNum)
        return index_buffer[thid] + index_buffer_num[thid] * kSize8B;
    else
    {
        write(index_write_fd_dio[thid], index_buffer[thid], kIndexBufferSize);
        index_buffer_num[thid] = 0;
        return index_buffer[thid];
    }
}

static inline int __index_read_BIO(char *buf, const int32_t off, const int len, const uint8_t thid)
{
    int ret = pread(index_read_fd_bio[thid], buf, len, off);

    if (ret < 0)
    {
        ERROR("errno=%d, errmsg=%s", errno, strerror(errno));
        return -1;
    }
    else if (ret < len)
    {
        int32_t buffer_len = index_buffer_num[thid] * kSize8B;

        if (ret > 0)
            memcpy(buf + ret, index_buffer[thid], __MIN(len - ret, buffer_len));
        else
            memcpy(buf, index_buffer[thid] + off - (memory_kv_num[thid] * kSize8B - buffer_len), __MIN(len, buffer_len));
    }

    return 0;
}

static inline char *__data_WAL_prepare(const uint8_t thid)
{
    if (data_buffer_num[thid] < kDataBufferNum)
        return data_buffer[thid] + data_buffer_num[thid] * 4096;
    else
    {
        write(data_write_fd_dio[thid], data_buffer[thid], kDataBufferSize);
        data_buffer_num[thid] = 0;
        return data_buffer[thid];
    }
}

static inline int __data_read_BIO(char *buf, const int64_t off, const int len, const uint8_t thid)
{
    int ret = pread64(data_read_fd_bio[thid], buf, len, off);

    if (ret < 0)
    {
        ERROR("errno=%d, errmsg=%s", errno, strerror(errno));
        return -1;
    }
    else if (ret < len)
    {
        int32_t buffer_len = data_buffer_num[thid] * 4096;

        if (ret > 0)
            memcpy(buf + ret, data_buffer[thid], __MIN(len - ret, buffer_len));
        else
            memcpy(buf, data_buffer[thid] + off - (memory_kv_num[thid] * kSize4K - buffer_len), __MIN(len, buffer_len));
    }

    return 0;
}

static void __do_set(int fd, uint8_t thid)
{
    int ret;
    struct iovec iov[2];
    int8_t flag = 0;
    struct pollfd poll_fd;
    poll_fd.fd = fd;

    while (1)
    {
        iov[0].iov_base = __data_WAL_prepare(thid);//mmap&disk
        iov[0].iov_len = kSize4K;
        iov[1].iov_base = __index_WAL_prepare(thid);//mmap&disk
        iov[1].iov_len = kSize8B;
        //setsockopt(fd, IPPROTO_TCP, TCP_QUICKACK, &kOn, sizeof (kOn));
        ret = __readv_with_poll(&poll_fd, iov, 2);
        if (ret <= 0)
            return;

        mmap_counter_arr[thid]++;//mmap&disk
        index_buffer_num[thid]++;//memory
        data_buffer_num[thid]++;//memory
        memory_kv_num[thid]++;//memory
        setsockopt(fd, IPPROTO_TCP, TCP_QUICKACK, &kOn, sizeof (kOn));
        __write_with_poll(&poll_fd, &flag, kSize1B);
    }
}

static void __do_get(const int fd, const int thid)
{
    int ret;
    uint64_t info;
    char data[kTCPDataPackage];
    struct timeval tv1, tv2;
    long long cost;
    int count;
    struct pollfd poll_fd;
    poll_fd.fd = fd;

    static const int32_t num_mod = (1 << 22) - 1;

    while (1)
    {
        ret = __read_with_poll(&poll_fd, &info, kSize8B);
        if (ret < 0)
        {
            ERROR("errno=%d, errmsg=%s", errno, strerror(errno));
            return;
        }
        else if (ret == 0)
        {
            ERROR("connection[%d] is closed", thid);
            return;
        }

        //[seqid|num|fileid|iorv]
        int8_t is_value = info & 1;
        info >>= 1;
        int8_t fileid = info & 0xf;
        info >>= 4;
        int32_t num = info & num_mod;
        int32_t seqid = info >> 22;

        int64_t each_size = (is_value ? kSize4K : kSize8B);
        int64_t off = seqid * each_size;
        int64_t left = num * each_size;
        count = 0;
        cost = 0;

        while (left > 0)
        {
            gettimeofday(&tv1, NULL);
            int sz = __MIN(left, kTCPDataPackage);

            if (is_value)
                __data_read_BIO(data, off, sz, fileid);
            else
                __index_read_BIO(data, off, sz, fileid);

            ret = __write_with_poll(&poll_fd, data, sz);
            if (ret < 0)
            {
                ERROR("errno=%d, errmsg=%s", errno, strerror(errno));
                return;
            }
            else if (ret == 0)
            {
                ERROR("connection[%d] is closed", thid);
                return;
            }

            left -= sz;
            off += sz;
            gettimeofday(&tv2, NULL);
            cost += ((long long)tv2.tv_sec - tv1.tv_sec) * 1000000 + tv2.tv_usec - tv1.tv_usec;
            count++;
            if (count > 1 && (count - 1) % 128 == 0)
            {
                if (cost < 3880)
                    usleep(3880 - cost);

                cost = 0;
            }
        }
    }
}

static void *__set_routine(void *arg)
{
    uint8_t thid = *((uint8_t *)arg);
    int listenfd = __server_listen(server_ip, kPortStartForSet + thid, 1);

    int ret;
    struct pollfd poll_listenfd;
    poll_listenfd.fd = listenfd;
    poll_listenfd.events = POLLIN | POLLOUT;

    while (1)
    {
        int nready = poll(&poll_listenfd, 1, -1);
        if (nready < 0)
        {
            ERROR("errno=%d, errmsg=%s", errno, strerror(errno));
            break;
        }
        else if (nready == 0)
        {
            continue;
        }

        int connfd = accept(listenfd, NULL, NULL);
        if (connfd < 0)
        {
            ERROR("errno=%d, errmsg=%s", errno, strerror(errno));
            break;
        }
        else
        {
            if (__set_fd_nonblock(connfd) < 0)
            {
                ERROR("errno=%d, errmsg=%s", errno, strerror(errno));
                break;
            }

            setsockopt(connfd, IPPROTO_TCP, TCP_NODELAY, &kOn, sizeof (kOn));
            DEBUG("set_routine[%d] accepted one connect, fd=%d", thid, connfd);
            FLUSH(stdout);
            if (thid == 0)
            {
                struct pollfd poll_fd;
                poll_fd.fd = connfd;

                if (thid == 0)
                {
                    ret = __write_with_poll(&poll_fd, memory_kv_num, kSize4B * kThreadMax);
                    if (ret < 0)
                    {
                        ERROR("errno=%d, errmsg=%s", errno, strerror(errno));
                        break;
                    }
                    else if (ret == 0)
                    {
                        ERROR("connection is closed");
                        break;
                    }
                }
            }

            __do_set(connfd, thid);
            close(connfd);
            FLUSH(stdout);
        }
    }

    DEBUG("set_routine[%d] exit", thid);
    close(listenfd);
    return NULL;
}

static void *__get_routine(void *arg)
{
    uint8_t thid = *((uint8_t *)arg);
    int listenfd = __server_listen(server_ip, kPortStartForGet + thid, 1);

    struct pollfd poll_listenfd;
    poll_listenfd.fd = listenfd;
    poll_listenfd.events = POLLIN | POLLOUT;

    while (1)
    {
        int nready = poll(&poll_listenfd, 1, -1);
        if (nready < 0)
        {
            ERROR("errno=%d, errmsg=%s", errno, strerror(errno));
            break;
        }
        else if (nready == 0)
        {
            continue;
        }

        int connfd = accept(listenfd, NULL, NULL);
        if (connfd < 0)
        {
            ERROR("errno=%d, errmsg=%s", errno, strerror(errno));
            break;
        }
        else
        {
            if (__set_fd_nonblock(connfd) < 0)
            {
                ERROR("errno=%d, errmsg=%s", errno, strerror(errno));
                break;
            }

            //setsockopt(connfd, IPPROTO_TCP, TCP_NODELAY, &kOn, sizeof (kOn));
            DEBUG("get_routine[%d] accepted one connect, fd=%d", thid, connfd);
            FLUSH(stdout);
            __do_get(connfd, thid);
            close(connfd);
            FLUSH(stdout);
        }
    }

    DEBUG("get_routine[%d] exit", thid);
    close(listenfd);
    return NULL;
}

static int __init_server(const char *ip, const char *dir, const int clear)
{
    DEBUG("-DB CREATE-");
    DEBUG("kDataBufferSize %d", kDataBufferSize);
    DEBUG("kIndexBufferSize %d", kIndexBufferSize);
    DEBUG("kTCPDataPackage %d", kTCPDataPackage);
    DEBUG("kCacheBlockNum %d", kCacheBlockNum);

    for (uint8_t i = 0; i < kThreadMax; i++)
        kDigit[i] = i;

    server_ip = ip;

    if (access(dir, F_OK) != 0 && mkdir(dir, 0755) < 0)
    {
        ERROR("errno=%d, errmsg=%s", errno, strerror(errno));
        return -1;
    }
    else if (clear)
    {
        DIR *dp = opendir(dir);
        if (!dp)
            return -1;

        char path[NAME_MAX] = {0};
        int pos = strlen(dir);
        strncpy(path, dir, pos);
        if (dir[pos - 1] != '/')
        {
            pos++;
            strcat(path, "/");
        }

        struct dirent *item;
        while ((item = readdir(dp)) != NULL)
        {
            if (!strcmp(item->d_name, ".")  ||
                !strcmp(item->d_name, "..") ||
                (item->d_type & DT_DIR) == DT_DIR)
            {
                continue;
            }

            strncpy(path + pos, item->d_name, NAME_MAX - pos);
            unlink(path);
        }

        closedir(dp);
    }

    char file_name[256];
    int fd;
    int is_new;
    void *ptr;

    /* init counter */
    sprintf(file_name, "%s/COUNTER", dir);
    fd = open(file_name, O_RDWR | O_CREAT, 0644);
    if (fd < 0)
    {
        ERROR("errno=%d, errmsg=%s", errno, strerror(errno));
        return -1;
    }

    is_new = 0;
    if (GetFileLength(file_name) != kSize4K)
    {
        is_new = 1;
        if (ftruncate(fd, kSize4K) < 0)
        {
            ERROR("errno=%d, errmsg=%s", errno, strerror(errno));
            close(fd);
            return -1;
        }
    }

    ptr = mmap(NULL, kSize4K, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    close(fd);

    if (ptr == (void *)-1)
    {
        ERROR("errno=%d, errmsg=%s", errno, strerror(errno));
        return -1;
    }

    if (is_new)
        memset(ptr, 0, kSize4K);

    mmap_counter_arr = (int32_t *)ptr;
    memcpy(memory_kv_num, mmap_counter_arr, sizeof (memory_kv_num));
    DEBUG("CounterKV init succ %s", dir);

    /* init index */
    for (int i = 0; i < kThreadMax; i++)
    {
        sprintf(file_name, "%s/INDEX.%02X", dir, i);
        fd = open(file_name, O_WRONLY | O_APPEND | O_DIRECT | O_CREAT, 0644);
        if (fd < 0)
        {
            ERROR("errno=%d, errmsg=%s", errno, strerror(errno));
            return -1;
        }

        index_write_fd_dio[i] = fd;

        int32_t index_num = GetFileLength(file_name) / kSize8B;

        index_read_fd_bio[i] = open(file_name, O_RDONLY | O_NOATIME);

        //-----------buffer
        sprintf(file_name, "%s/BUFFER.INDEX.%02X", dir, i);
        fd = open(file_name, O_RDWR | O_CREAT, 0644);
        if (fd < 0)
        {
            ERROR("errno=%d, errmsg=%s", errno, strerror(errno));
            return -1;
        }

        is_new = 0;
        if (GetFileLength(file_name) != kIndexBufferSize)
        {
            is_new = 1;
            if (ftruncate(fd, kIndexBufferSize) < 0)
            {
                ERROR("errno=%d, errmsg=%s", errno, strerror(errno));
                close(fd);
                return -1;
            }
        }

        ptr = mmap(NULL, kIndexBufferSize, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
        close(fd);

        if (ptr == (void *)-1)
        {
            ERROR("errno=%d, errmsg=%s", errno, strerror(errno));
            return -1;
        }

        if (is_new)
            memset(ptr, 0, kIndexBufferSize);

        index_buffer[i] = (char *)ptr;
        index_buffer_num[i] = memory_kv_num[i] - index_num;
    }

    DEBUG("IndexTable init succ %s", dir);

    /* init data */
    for (int i = 0; i < kThreadMax; i++)
    {
        //----------data
        sprintf(file_name, "%s/DATA.%02X", dir, i);
        fd = open(file_name, O_RDONLY | O_LARGEFILE | O_NOATIME | O_CREAT, 0644);
        if (fd < 0)
        {
            ERROR("errno=%d, errmsg=%s", errno, strerror(errno));
            return -1;
        }

        data_read_fd_bio[i] = fd;
        int32_t value_count = (int32_t)(GetFileLength(file_name) / kSize4K);
        if (value_count > 0)
        {
            DEBUG("Value Count[%d]: %d", i, value_count);
        }

        fd = open(file_name, O_WRONLY | O_LARGEFILE | O_APPEND | O_DIRECT);
        if (fd < 0)
        {
            ERROR("errno=%d, errmsg=%s", errno, strerror(errno));
            return -1;
        }

        data_write_fd_dio[i] = fd;

        //-----------buffer
        sprintf(file_name, "%s/BUFFER.DATA.%02X", dir, i);
        fd = open(file_name, O_RDWR | O_CREAT, 0644);
        if (fd < 0)
        {
            ERROR("errno=%d, errmsg=%s", errno, strerror(errno));
            return -1;
        }

        is_new = 0;
        if (GetFileLength(file_name) != kDataBufferSize)
        {
            is_new = 1;
            if (ftruncate(fd, kDataBufferSize) < 0)
            {
                ERROR("errno=%d, errmsg=%s", errno, strerror(errno));
                close(fd);
                return -1;
            }
        }

        ptr = mmap(NULL, kDataBufferSize, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
        close(fd);

        if (ptr == (void *)-1)
        {
            ERROR("errno=%d, errmsg=%s", errno, strerror(errno));
            return -1;
        }

        if (is_new)
            memset(ptr, 0, kDataBufferSize);

        data_buffer[i] = (char *)ptr;
        data_buffer_num[i] = memory_kv_num[i] - value_count;

        if (data_buffer_num[i] > 0)
        {
            DEBUG("*UPDATE* Value Count[%d] add %d", i, data_buffer_num[i]);
        }
    }

    DEBUG("DataTable thread init succ %s", dir);

    return 0;
}

static void __start_server()
{
    for (uint8_t i = 0; i < kThreadMax; i++)
        pthread_create(&server_set_threads[i], NULL, __set_routine, &kDigit[i]);

    for (uint8_t i = 0; i < kThreadMax; i++)
        pthread_create(&server_get_threads[i], NULL, __get_routine, &kDigit[i]);
}

static void __stop_server()
{
    for (int i = 0; i < kThreadMax; i++)
    {
        if (server_set_threads[i] >= 0)
        {
            pthread_cancel(server_set_threads[i]);
            pthread_join(server_set_threads[i], NULL);
        }

        if (server_get_threads[i] >= 0)
        {
            pthread_cancel(server_get_threads[i]);
            pthread_join(server_get_threads[i], NULL);
        }
    }

    for (int i = 0; i < kThreadMax; i++)
    {
        if (index_read_fd_bio[i] >= 0)
            close(index_read_fd_bio[i]);

        if (index_write_fd_dio[i] >= 0)
        {
            close(index_write_fd_dio[i]);
            index_write_fd_dio[i] = -1;
        }

        if (index_buffer[i])
            munmap(index_buffer[i], kIndexBufferSize);

        if (data_read_fd_bio[i] >= 0)
            close(data_read_fd_bio[i]);

        if (data_write_fd_dio[i] >= 0)
            close(data_write_fd_dio[i]);

        if (data_buffer[i])
            munmap(data_buffer[i], kDataBufferSize);
    }

    if (mmap_counter_arr)
        munmap(mmap_counter_arr, kSize4K);
}

static const char *exeName(const char *name) {
    int pos = 0;
    if (name == NULL || (pos = strlen(name)) < 1) {
        return NULL;
    }

    for (; pos > 0; pos--) {
        if (name[pos - 1] == '/') {
            break;
        }
    }

    return name + pos;
}

static void help(const char *name) {
    printf("usage: %s host file_dir [clear]\n", name);
    printf("   eg: %s tcp://0.0.0.0 ./data", name);
    printf("   eg: %s tcp://0.0.0.0 ./data clear", name);
    exit(-1);
}

static void pause_here() {
    //system("stty raw -echo");
    printf("press any key to exit ...\n");
    getchar();
    //system("stty -raw echo");
}

int main(int argc, char * argv[]) {
    const char * name = exeName(argv[0]);

    if (argc != 3 && argc != 4) {
        printf("param should be 3 or 4\n");
        help(name);
        return -1;
    }

    const char * host = argv[1];
    const char * dir  = argv[2];
    int clear = 0;
    if (argc == 4) {
        if (strcmp("clear", argv[3]) != 0) {
            printf("param [4] should be \"clear\" if you want to clear local data\n");
            help(name);
            return -1;
        } else {
            clear = 1;
        }
    }

    printf("[%s] ***Compass DBServer***\n", name);
    printf("  >> bind  host : %s\n", host);
    printf("  >> data  dir  : %s\n", dir);
    printf("  >> clear dir  : %d\n", clear);

    ////////////////////////////////////////////////////

    if (__init_server(host + 6, dir, clear) == 0)
    {
        printf("server try to start...\n");
        __start_server();
        printf("server start succ\n");
        pause_here();
        printf("server try to stop...\n");
        __stop_server();
        printf("server stop succ\n");
    }

    return 0;
}

