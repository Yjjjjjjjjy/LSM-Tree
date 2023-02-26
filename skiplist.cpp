#include "skiplist.h"
#include <cstring>

double SkipList::my_rand()
{
    s = (16807 * s) % 2147483647ULL;
    return (s + 0.0) / 2147483647ULL;
}

int SkipList::randomLevel()
{
    int result = 1;
    while (result < MAX_LEVEL && my_rand() < 0.5)
    {
        ++result;
    }
    return result;
}

void SkipList::Insert(uint64_t key, std::string value)
{
    SKNode *update[MAX_LEVEL];
    SKNode *NewNode;
    for (int i = 0; i < MAX_LEVEL; ++i)
    {
        update[i] = head;
    }
    SKNode *x = head;
    int num = MAX_LEVEL;
    for (int i = num - 1; i >= 0; --i)
    {
        while (x->forwards[i]->type != 3 && x->forwards[i]->key <= key)
        {
            x = x->forwards[i];
        }
        update[i] = x;
    }
    x = update[0];
    if (x->type == NORMAL && x->key == key)
    {
        cacheSize = cacheSize - x->val.size() + value.size();
        x->val = value;
        return;
    }
    else
    {
        int lvl = randomLevel();
        NewNode = new SKNode(key, value, NORMAL);
        for (int i = 0; i < lvl; ++i)
        {
            NewNode->forwards[i] = update[i]->forwards[i];
            update[i]->forwards[i] = NewNode;
        }
        cacheSize += 12 + value.size();
        ++length;
    }
}

std::string SkipList::Search(uint64_t key)
{
    SKNode *x = head;
    hot = NULL;
    int num = MAX_LEVEL;
    for (int i = num - 1; i >= 0; --i)
    {
        if (x->forwards[i]->type == 3)
        {
            if (i > 0)
                continue;
        }
        while (x->forwards[i]->type == NORMAL && x->forwards[i]->key < key)
        {
            x = x->forwards[i];
        }
    }
    if ((x->forwards[0]->type == NORMAL) && (x->forwards[0]->key == key))
    {
        hot = x->forwards[0];
        return hot->val;
    }
    else
        return "";
}


bool SkipList::scanSearch(uint64_t key_start, uint64_t key_end, std::list<std::pair<uint64_t, std::string>> &list)
{
    SKNode *x = head;
    int num = MAX_LEVEL;
    for (int i = num - 1; i >= 0; --i)
    {
        if (x->forwards[i]->type == 3)
            continue;
        while (x->forwards[i]->type == NORMAL && x->forwards[i]->key < key_start)
        {
            x = x->forwards[i];
        }
    }
    if ((x->forwards[0]->type == NORMAL) && (x->forwards[0]->key >= key_start) && (x->forwards[0]->key <= key_end))
    {
        do
        {
            std::pair<uint64_t, std::string> temp(x->forwards[0]->key, x->forwards[0]->val);
            list.push_back(temp);
            x = x->forwards[0];
        } while (x->forwards[0]->type == NORMAL && x->forwards[0]->key <= key_end);
    }
    if (list.empty())
        return false;
    return true;
}

SSTableCache *SkipList::transform(const std::string &dir, const uint64_t &currentTime)
{
    SSTableCache *cache = new SSTableCache();
    char *buffer = new char[cacheSize];
    cache->Header.timestamp = currentTime;
    cache->Header.num = length;
    *(uint64_t *)buffer = currentTime;
    SKNode *x = head;
    x = x->forwards[0];
    *(uint64_t *)(buffer + 8) = length;
    *(uint64_t *)(buffer + 16) = x->key;
    char *index = buffer + 10272;
    uint64_t offerset = 10272 + 12 * length;
    while (x != NIL)
    {
        uint64_t key = x->key;
        cache->BF->setBF(key);
        INDEX temp;
        temp.Key = key;
        temp.Offset = offerset;
        *(uint64_t *)index = key;
        index += 8;
        *(uint32_t *)index = offerset;
        index += 4;
        cache->Index.push_back(temp);
        uint32_t strlen = x->val.size();
        if(strlen + offerset > cacheSize)
        {
            cout<<"buffer flow !"<<endl;
            exit(-1);
        }
        memcpy(buffer + offerset, (x->val).c_str(), strlen);
        offerset += strlen;
        x = x->forwards[0];
    }

    cache->Header.max = cache->Index[cache->Header.num - 1].Key;
    *(uint64_t *)(buffer + 24) = cache->Header.max;
    cache->Header.min = cache->Index[0].Key;
    std::string fileName = dir + "/" + std::to_string(currentTime) + ".sst";
    cache->path = fileName;

    cache->BF->saveBuffer(buffer + 32);
    std::ofstream outFile(fileName, std::ios::binary | std::ios::out);
    outFile.write(buffer, cacheSize);
    delete[] buffer;
    outFile.close();
    return cache;
}

bool SkipList::needTransform(uint64_t key, std::string value)
{
    string s = Search(key);
    int size = cacheSize;
    if (s == "")
    {
        size += 12 + value.size();
    }
    else
    {
        size = size - hot->val.size() + value.size();
    }
    if (size > MAX_TABLE_SIZE)
        return true;
    else
        return false;
}


