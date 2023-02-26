#ifndef SKIPLIST_H
#define SKIPLIST_H

#include <iostream>
#include <vector>
#include <climits>
#include "sstable.h"
#include <list>
#include <fstream>

#define MAX_LEVEL 8

enum SKNodeType
{
    HEAD = 1,
    NORMAL,
    NIL
};

struct SKNode
{
    uint64_t key;
    std::string val;
    SKNodeType type;
    std::vector<SKNode *> forwards;
    SKNode(uint64_t _key, std::string _val, SKNodeType _type)
        : key(_key), val(_val), type(_type)
    {
        for (int i = 0; i < MAX_LEVEL; ++i)
        {
            forwards.push_back(nullptr);
        }
    }
};

class SkipList
{
private:
    SKNode *head;
    SKNode *NIL;

    uint64_t s = 1;
    double my_rand();
    int randomLevel();

public:
    uint64_t cacheSize;
    uint32_t length;
    SkipList()
    {
        head = new SKNode(0, "", SKNodeType::HEAD);
        NIL = new SKNode(INT_MAX, "", SKNodeType::NIL);
        cacheSize = 10272;
        length = 0;
        for (int i = 0; i < MAX_LEVEL; ++i)
        {
            head->forwards[i] = NIL;
        }
    }
    SKNode *hot;
    void Insert(uint64_t key, std::string value);
    std::string Search(uint64_t key);
    bool scanSearch(uint64_t key_start, uint64_t key_end, std::list<std::pair<uint64_t, std::string>> &list);
    SSTableCache *transform(const std::string &dir, const uint64_t &currentTime);
    bool needTransform(uint64_t key, string value);
    ~SkipList()
    {
        SKNode *n1 = head;
        SKNode *n2;
        while (n1)
        {
            n2 = n1->forwards[0];
            delete n1;
            n1 = n2;
        }
    }
};

#endif // SKIPLIST_H
