#ifndef BLOOMFILTER_H
#define BLOOMFILTER_H

#include <bitset>
#include "MurmurHash3.h"
#include <cstring>

using std::bitset;

class BloomFilter
{
private:
    bitset<81920> BF;

public:
    BloomFilter();
    BloomFilter(char *buf);

    unsigned int hash[4] = {0};
    void setBF(uint64_t key);
    bool isExisted(uint64_t key);
    void saveBuffer(char *buf);
};

#endif // BLOOMFILTER_H
