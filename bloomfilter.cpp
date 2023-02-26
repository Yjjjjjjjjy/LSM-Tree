#include "bloomfilter.h"

BloomFilter::BloomFilter()
{
    BF.reset();
}

BloomFilter::BloomFilter(char *buf)
{
    memcpy((char *)&BF, buf, 10240);
}

bool BloomFilter::isExisted(uint64_t key)
{
    MurmurHash3_x64_128(&key, sizeof(key), 1, hash);
    for (int i = 0; i < 4; ++i)
    {
        hash[i] %= 81920;
        if (!BF[hash[i]])
            return false;
    }
    return true;
}

void BloomFilter::setBF(uint64_t key)
{
    MurmurHash3_x64_128(&key, sizeof(key), 1, hash);
    for (int i = 0; i < 4; ++i)
    {
        hash[i] %= 81920;
        BF[hash[i]] = 1;
    }
}

void BloomFilter::saveBuffer(char *buf)
{
    memcpy(buf, (char *)&BF, 10240);
}
