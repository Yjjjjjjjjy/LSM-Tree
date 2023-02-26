#include "sstable.h"

SSTableCache::SSTableCache()
{
    BF = new BloomFilter();
}

SSTableCache::SSTableCache(const std::string &dir)
{
    path = dir;
    std::ifstream file(dir, std::ios::binary);
    if (!file)
    {
        printf("Fail to open file %s", dir.c_str());
        exit(-1);
    }
    file.read((char *)&Header.timestamp, 8);
    file.read((char *)&Header.num, 8);
    file.read((char *)&Header.min, 8);
    file.read((char *)&Header.max, 8);
    char *filterBuf = new char[10240];
    file.read(filterBuf, 10240);
    BF = new BloomFilter(filterBuf);
    uint64_t length = Header.num;
    char *indexBuf = new char[length * 12];
    file.read(indexBuf, length * 12);
    for (unsigned i = 0; i < length; ++i)
    {
        Index.push_back(INDEX(*(uint64_t *)(indexBuf + 12 * i), *(uint64_t *)(indexBuf + 12 * i + 8)));
        ;
    }
    delete[] filterBuf;
    delete[] indexBuf;
    file.close();
}

int SSTableCache::search(uint64_t key)
{
    if (key <= Header.max && key >= Header.min && BF->isExisted(key))
    {
        return find(key, 0, Index.size() - 1);
    }
    else
        return -1;
}

int SSTableCache::find(uint64_t key, int lo, int hi)
{
    if (lo > hi)
        return -1;
    if (lo == hi)
    {
        if (Index[lo].Key == key)
            return lo;
        else
            return -1;
    }
    int mi = (hi + lo) >> 1;
    if (Index[mi].Key == key)
        return mi;
    else if (Index[mi].Key < key)
        return find(key, mi + 1, hi);
    else
        return find(key, lo, mi - 1);
}

int SSTableCache::lowpos(uint64_t key1, uint64_t key2)
{
    if (key1 > (Header).max || key2 < (Header).min)
        return -1;
    uint64_t lo, hi;
    lo = max((Header).min, key1);
    hi = min((Header).max, key2);
    int Lowpos = find2(0, Header.num - 1, lo, hi);
    if (Lowpos == -1)
        return -1;
    while (Lowpos != 0)
    {
        if (Index[Lowpos - 1].Key >= key1 && Index[Lowpos - 1].Key <= key2)
            Lowpos--;
        else
            break;
    }
    return Lowpos;
}

int SSTableCache::find2(int lo, int hi, uint64_t key1, uint64_t key2)
{
    if (lo == hi)
    {
        if (Index[lo].Key >= key1 && Index[lo].Key <= key2)
            return lo;
        else
            return -1;
    }
    int mi = (hi + lo) >> 1;
    if (Index[mi].Key >= key1 && Index[mi].Key <= key2)
        return mi;
    else if (Index[mi].Key > key2)
        return find2(lo, mi - 1, key1, key2);
    else
        return find2(mi + 1, hi, key1, key2);
}

SSTable::SSTable(SSTableCache *cache)
{
    path = cache->path;
    std::ifstream file(path, std::ios::binary);
    if (!file)
    {
        printf("Fail to open file %s", path.c_str());
        exit(-1);
    }
    timeStamp = cache->Header.timestamp;
    length = (cache->Header).num;
    file.seekg((cache->Index)[0].Offset);
    for (auto it = (cache->Index).begin();;)
    {
        uint64_t key = (*it).Key;
        uint32_t offset = (*it).Offset;
        std::string val;
        if (++it == (cache->Index).end())
        {
            file >> val;
            Entries.push_back(std::pair<uint64_t, std::string>(key, val));
            break;
        }
        else
        {
            uint32_t length = (*it).Offset - offset;
            char *buf = new char[length + 1];
            buf[length] = '\0';
            file.read(buf, length);
            val = buf;
            delete[] buf;
            Entries.push_back(std::pair<uint64_t, std::string>(key, val));
        }
    }
    delete cache;
}

void SSTable::merge(std::vector<SSTable> &tables)
{
    uint64_t size = tables.size();
    if (size == 1)
        return;
    uint32_t group = size / 2;
    std::vector<SSTable> next;
    for (uint32_t i = 0; i < group; ++i)
    {
        next.push_back(merge2(tables[2 * i], tables[2 * i + 1]));
    }
    if (size % 2 == 1)
        next.push_back(tables[size - 1]);
    merge(next);
    tables = next;
}

std::vector<SSTableCache *> SSTable::save(const std::string &dir)
{
    std::vector<SSTableCache *> caches;
    SSTable newTable;
    uint64_t num = 0;
    while (!Entries.empty())
    {
        if (newTable.size + 12 + Entries.front().second.size() >= MAX_TABLE_SIZE)
        {
            caches.push_back(newTable.saveSingle(dir, timeStamp, num++));
            newTable = SSTable();
        }
        newTable.add(Entries.front());
        Entries.pop_front();
    }
    if (newTable.length > 0)
    {
        caches.push_back(newTable.saveSingle(dir, timeStamp, num));
    }
    return caches;
}

SSTable SSTable::merge2(SSTable &a, SSTable &b)
{
    SSTable result;
    result.timeStamp = a.timeStamp;
    while ((!a.Entries.empty()) && (!b.Entries.empty()))
    {
        uint64_t aKey = a.Entries.front().first, bKey = b.Entries.front().first;
        if (aKey > bKey)
        {
            result.Entries.push_back(b.Entries.front());
            b.Entries.pop_front();
        }
        else if (aKey < bKey)
        {
            result.Entries.push_back(a.Entries.front());
            a.Entries.pop_front();
        }
        else
        {
            result.Entries.push_back(a.Entries.front());
            a.Entries.pop_front();
            b.Entries.pop_front();
        }
    }
    while (!a.Entries.empty())
    {
        result.Entries.push_back(a.Entries.front());
        a.Entries.pop_front();
    }
    while (!b.Entries.empty())
    {
        result.Entries.push_back(b.Entries.front());
        b.Entries.pop_front();
    }
    return result;
}

bool cacheTimeCompare(SSTableCache *a, SSTableCache *b)
{
    return (a->Header).timestamp > (b->Header).timestamp;
}

bool haveIntersection(const SSTableCache *cache, const std::vector<range> &ranges)
{
    unsigned long long min = (cache->Header).min;
    unsigned long long max = (cache->Header).max;
    for (auto it = ranges.begin(); it != ranges.end(); ++it)
    {
        if (!((*it).max < min || (*it).min > max))
        {
            return true;
        }
    }
    return false;
}

bool tableTimecompare(SSTable &a, SSTable &b)
{
    return a.timeStamp > b.timeStamp;
}

void SSTable::add(std::pair<uint64_t, std::string> &entry)
{
    size += 12 + entry.second.size();
    length++;
    Entries.push_back(entry);
}

SSTableCache *SSTable::saveSingle(const std::string &dir, const uint64_t &currentTime, const uint64_t &num)
{
    SSTableCache *cache = new SSTableCache;

    char *buffer = new char[size];
    BloomFilter *filter = cache->BF;

    *(uint64_t *)buffer = currentTime;
    (cache->Header).timestamp = currentTime;

    *(uint64_t *)(buffer + 8) = length;
    (cache->Header).num = length;

    *(uint64_t *)(buffer + 16) = Entries.front().first;
    (cache->Header).min = Entries.front().first;

    char *index = buffer + 10272;
    uint32_t offset = 10272 + length * 12;

    for (auto it = Entries.begin(); it != Entries.end(); ++it)
    {
        filter->setBF((*it).first);
        *(uint64_t *)index = (*it).first;
        index += 8;
        *(uint32_t *)index = offset;
        index += 4;

        (cache->Index).push_back(INDEX((*it).first, offset));
        uint32_t strLen = ((*it).second).size();
        uint32_t newOffset = offset + strLen;
        if (newOffset > size)
        {
            printf("Buffer Overflow!!!\n");
            exit(-1);
        }
        memcpy(buffer + offset, ((*it).second).c_str(), strLen);
        offset = newOffset;
    }

    *(uint64_t *)(buffer + 24) = Entries.back().first;
    (cache->Header).max = Entries.back().first;
    filter->saveBuffer(buffer + 32);

    std::string filename = dir + "/" + std::to_string(currentTime) + "-" + std::to_string(num) + ".sst";
    cache->path = filename;
    std::ofstream outFile(filename, std::ios::binary | std::ios::out);
    outFile.write(buffer, size);

    delete[] buffer;
    outFile.close();
    return cache;
}
