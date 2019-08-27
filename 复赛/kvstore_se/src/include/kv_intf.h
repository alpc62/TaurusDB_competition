#pragma once

#include <memory>

#include "kv_string.h"

class KVIntf {
public:
    virtual ~KVIntf() {};

    virtual bool Init(const char * host, int id) = 0;

    virtual void Close() = 0;

    virtual int Set(KVString &key, KVString & val) = 0;

    virtual int Get(KVString &key, KVString & val) = 0;
};

extern "C" std::shared_ptr<KVIntf> GetKVIntf();
