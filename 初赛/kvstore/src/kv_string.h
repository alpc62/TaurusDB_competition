#pragma once

#include <string.h>

class KVString {
public:
    KVString(const char * str) {
        if (str != nullptr) {
            len_ = strlen(str) + 1;
            buf_ = new char[len_];
            memcpy(buf_, str, len_);
        }
    }

    KVString(const char * str, int len) {
        if (str != nullptr) {
            len_ = len;
            buf_ = new char[len_];
            memcpy(buf_, str, len_);
        }
    }

    KVString() = default;

    KVString(const KVString & other) {
        delete buf_;
        len_ = other.len_;
        buf_ = new char[len_];
        memcpy(buf_, other.buf_, len_);
    }

    KVString(KVString && other) {
        delete []buf_;
        len_ = other.len_;
        buf_ = other.buf_;
        other.buf_ = nullptr;
        other.len_ = 0;
    }

    ~KVString() {
        delete [] buf_;
    }

    KVString & operator=(const KVString & other) {
        if(this != &other) {
            delete []buf_;
            len_ = other.len_;
            buf_ = new char[len_];
            memcpy(buf_, other.buf_, len_);
        }
        return *this;
    }

    KVString & operator+(const KVString & other) {
        char * buf = new char[other.len_ + len_];
        memcpy(buf, buf_, len_);
        memcpy(buf + len_, other.buf_, other.len_);
        Reset(buf, len_ + other.len_);
        return *this;
    }

    KVString & operator+(const char * other) {
        int size = strlen(other) + len_;
        char * buf = new char[size];
        memcpy(buf, buf_, len_);
        strcat(buf, other);
        Reset(buf, size);
        return *this;
    }


    KVString & operator=(const char * str) {
        if (str != nullptr) {
            len_ = strlen(str) + 1;
            buf_ = new char[len_];
            memcpy(buf_, str, len_);
        }
        return *this;
    }

    KVString & operator=(KVString && other) {
        if(this != &other) {
            delete []buf_;
            len_ = other.len_;
            buf_ = other.buf_;
            other.len_ = 0;
            other.buf_ = nullptr;
        }
        return *this;
    }

    KVString Sub(int begin, int end) {
        if ( begin < end && end <= len_) {
            return KVString(buf_ + begin, end - begin);
        } else {
            return KVString();
        }
    }

    KVString SubStr(int begin, int end) {
        char * buf;
        KVString sub;
        if ( begin < end && end <= len_) {
            buf = new char[end - begin + 1];
            memcpy(buf, buf_ + begin, end - begin);
            buf[end - begin] = '\0';
            sub.Reset(buf, end - begin + 1);
        } else {
            buf = new char[1];
            buf[0] = '\0';
            sub.Reset(buf, 1);
        }
        return sub;
    }

    inline const char * Buf() const {
        return buf_;
    }

    inline int Size() const {
        return len_;
    }

    void Reset(char * buf, int len) {
        if (buf_ != nullptr) {
            delete [] buf_;
        }
        buf_ = buf;
        len_ = len;
    }

    bool operator==(const KVString & other) {
        if (len_ != other.len_) {
            return false;
        }
        if (len_ > 0) {
           return memcmp(buf_, other.buf_, len_) == 0;
        }
        return true;
    }

    bool operator==(const char * other) {
        if (other == nullptr) {
            return buf_ == nullptr;
        }
        int len = strlen(other) + 1;
        if (len_ != len) {
            return false;
        }
        if (len_ > 0) {
            return memcmp(buf_, other, len) == 0;
        }
        return true;
    }

    bool operator<(const KVString & other) const {
        int size = len_ < other.len_ ? len_ : other.len_;
        int ret = memcmp(buf_, other.buf_, size);
        return ret == 0 ? len_ < other.len_ : ret < 0;
    }

    bool Prefix(const char * prefix) {
        if (prefix == nullptr) {
            return true;
        }
        int len = strlen(prefix);
        return (len_ >= len) && (memcmp(buf_, prefix, len) == 0);
    }

    bool Prefix(const KVString &prefix) {
        return (len_ >= prefix.len_) && (memcmp(buf_, prefix.buf_, prefix.len_) == 0);
    }

    bool Suffix(const char * suffix) {
        if (suffix == nullptr) {
            return true;
        }
        int len = strlen(suffix) + 1;
        return (len_ >= len) && (memcmp(buf_ + len_ - len, suffix, len) == 0);
    }

    bool Suffix(const KVString & suffix) {
        return (len_ >= suffix.len_) && (memcmp(buf_ + len_ - suffix.len_, suffix.buf_, suffix.len_) == 0);
    }

private:
    int    len_ = 0;
    char * buf_ = nullptr;
};

