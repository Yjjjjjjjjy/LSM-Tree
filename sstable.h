#ifndef SSTABLE_H
#define SSTABLE_H

#include "bloomfilter.h"
#include <time.h>
#include <climits>
#include <vector>
#include <iostream>
#include <fstream>
#include <list>

#define MAX_TABLE_SIZE 2097152

using namespace std;

struct HEADER
{
    uint64_t timestamp;
    uint64_t num;
    uint64_t min;
    uint64_t max;
    HEADER()
    {
        timestamp = time(NULL);
        num = 0;
        min = INT_MAX;
        max = INT_MIN;
    }
};

struct INDEX
{
    uint64_t Key;
    uint32_t Offset;
    INDEX(uint64_t k = 0, uint32_t o = 0) : Key(k), Offset(o) {}
};

class SSTableCache
{
public:
    HEADER Header;
    BloomFilter *BF;
    vector<INDEX> Index;
    std::string path;
    SSTableCache();
    SSTableCache(const std::string &dir);
    int search(uint64_t key);
    int lowpos(uint64_t key1, uint64_t key2);
    ~SSTableCache() { delete BF; }

private:
    int find(uint64_t key, int lo, int hi);
    int find2(int lo, int hi, uint64_t key1, uint64_t key2);
};

struct range
{
    uint64_t min, max;
    range(uint64_t in, uint64_t ax) : min(in), max(ax) {}
};

class SSTable
{
public:
    uint64_t timeStamp;
    std::string path;
    uint64_t size;
    uint64_t length;
    std::list<std::pair<uint64_t, std::string>> Entries;
    SSTable(SSTableCache *cache);
    SSTable() : size(10272), length(0) {}
    std::vector<SSTableCache *> save(const std::string &dir);
    static void merge(std::vector<SSTable> &tables);
    static SSTable merge2(SSTable &a, SSTable &b);
    void add(std::pair<uint64_t, std::string> &);
    SSTableCache *saveSingle(const std::string &dir, const uint64_t &currentTime, const uint64_t &num);
};

bool cacheTimeCompare(SSTableCache *a, SSTableCache *b);
bool haveIntersection(const SSTableCache *cache, const std::vector<range> &ranges);
bool tableTimecompare(SSTable &a, SSTable &b);
#endif // SSTABLE_H
