#include "kvstore.h"
#include <string>
#include "utils.h"
#include <algorithm>

KVStore::KVStore(const std::string &dir) : KVStoreAPI(dir)
{
    dataDir = dir;
    currentTime = 0;
    if (utils::dirExists(dataDir))
    {
        std::vector<std::string> levelNames;
        int levelNum = utils::scanDir(dataDir, levelNames);
        if (levelNum > 0)
        {
            for (int i = 0; i < levelNum; ++i)
            {
                std::string levelName = "level-" + std::to_string(i);
                if (std::count(levelNames.begin(), levelNames.end(), levelName) == 1)
                {
                    cache.push_back(std::vector<SSTableCache *>());
                    std::string levelDir = dataDir + "/" + levelName;
                    std::vector<std::string> tableNames;
                    int tableNum = utils::scanDir(levelDir, tableNames);
                    for (int j = 0; j < tableNum; ++j)
                    {
                        SSTableCache *curCache = new SSTableCache(levelDir + "/" + tableNames[j]);
                        uint64_t curTime = (curCache->Header).timestamp;
                        cache[i].push_back(curCache);
                        if (curTime > currentTime)
                            currentTime = curTime;
                    }
                    std::sort(cache[i].begin(), cache[i].end(), cacheTimeCompare);
                }
                else
                    break;
            }
        }
        else
        {
            utils::mkdir((dataDir + "/level-0").c_str());
            cache.push_back(std::vector<SSTableCache *>());
        }
    }
    else
    {
        utils::mkdir(dataDir.c_str());
        utils::mkdir((dataDir + "/level-0").c_str());
        cache.push_back(std::vector<SSTableCache *>());
    }
    currentTime++;
    memTable = new SkipList();
}

KVStore::~KVStore()
{
    if (memTable->length > 0)
        memTable->transform(dataDir + "/level-0", currentTime++);
    delete memTable;
    compact();
    for (auto it1 = cache.begin(); it1 != cache.end(); ++it1)
    {
        for (auto it2 = (*it1).begin(); it2 != (*it1).end(); ++it2)
            delete (*it2);
    }
}

/**
 * Insert/Update the key-value pair.
 * No return values for simplicity.
 */
void KVStore::put(uint64_t key, const std::string &s)
{
    std::string tmp = s;
    if (memTable->needTransform(key, tmp))
    {
        cache[0].push_back(memTable->transform(dataDir + "/level-0", currentTime++));
        delete memTable;
        memTable = new SkipList;
        memTable->Insert(key, s);
        std::sort(cache[0].begin(), cache[0].end(), cacheTimeCompare);
        compact();
        return;
    }
    if (memTable->Search(key) == "")
        memTable->Insert(key, tmp);
    else
    {
        memTable->cacheSize = memTable->cacheSize - memTable->hot->val.size() + tmp.size();
        memTable->hot->val = tmp;

    }

}
/**
 * Returns the (string) value of the given key.
 * An empty string indicates not found.
 */
std::string KVStore::get(uint64_t key)
{

    std::string ret = memTable->Search(key);
    if (ret != "")
    {
        if (ret == "~DELETED~")
            return "";
        else
            return ret;
    }
    int levelNum = cache.size();
    for (int i = 0; i < levelNum; ++i)
    {
        for (auto it = cache[i].begin(); it != cache[i].end(); ++it)
        {
            int pos = (*it)->search(key);
            if (pos == -1)
                continue;
            std::ifstream file((*it)->path, std::ios::binary);
            if (!file)
            {
                printf("Lost file: %s", ((*it)->path).c_str());
                exit(-1);
            }
            std::string value;
            uint32_t length, offset = (*it)->Index[pos].Offset;
            file.seekg(offset);
            if ((uint32_t)pos == ((*it)->Index).size() - 1)
                file >> value;
            else
            {
                uint32_t nextOffset = (*it)->Index[pos + 1].Offset;
                length = nextOffset - offset;
                char *result = new char[length + 1];
                result[length] = '\0';
                file.read(result, length);
                value = result;
                delete[] result;
            }
            file.close();
            if (value != "~DELETED~")
                return value;
            else
                return "";
        }
    }
    return "";
}
/**
 * Delete the given key-value pair if it exists.
 * Returns false iff the key is not found.
 */
bool KVStore::del(uint64_t key)
{
    std::string val = get(key);
    if (val == "")
        return false;
    put(key, "~DELETED~");
    return true;
}

/**
 * This resets the kvstore. All key-value pairs should be removed,
 * including memtable and all sstables files.
 */
void KVStore::reset()
{
    delete memTable;
    memTable = new SkipList();
    string filePath = "";
    for (auto it1 = cache.begin(); it1 != cache.end(); ++it1)
    {
        for (auto it2 = (*it1).begin(); it2 != (*it1).end(); ++it2)
        {

            utils::rmfile((*it2)->path.c_str());
            if(filePath == "")
            {
                 filePath = (*it2)->path;
                 int pos = filePath.rfind('/');
                 filePath = filePath.substr(0,pos);
            }
            delete (*it2);
        }
        utils::rmdir(filePath.c_str());
        filePath = "";
    }
    cache.clear();
    cache.push_back(std::vector<SSTableCache *>());
}

/**
 * Return a list including all the key-value pair between key1 and key2.
 * keys in the list should be in an ascending order.
 * An empty string indicates not found.
 */
void KVStore::scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string>> &list)
{
    memTable->scanSearch(key1, key2, list);
    int levelNum = cache.size();
    for (int i = 0; i < levelNum; ++i)
    {
        for (auto it = cache[i].begin(); it != cache[i].end(); ++it)
        {

            int pos = (*it)->lowpos(key1, key2);
            if (pos == -1)
                continue;
            std::ifstream file((*it)->path, std::ios::binary);
            if (!file)
            {
                printf("Lost file: %s", ((*it)->path).c_str());
                exit(-1);
            }
            while (true)
            {
                uint64_t key = (*it)->Index[pos].Key;
                if (key <= key2 && key >= key1)
                {
                    string value;
                    uint32_t length, offset = (*it)->Index[pos].Offset;
                    file.seekg(offset);
                    if ((uint32_t)pos == ((*it)->Index).size() - 1)
                    {
                        file >> value;
                        list.push_back(std::pair<uint64_t, std::string>(key, value));
                        break;
                    }
                    uint32_t nextOffset = (*it)->Index[pos + 1].Offset;
                    length = nextOffset - offset;
                    char *result = new char[length + 1];
                    result[length] = '\0';
                    file.read(result, length);
                    value = result;
                    delete[] result;
                    list.push_back(std::pair<uint64_t, std::string>(key, value));
                    ++pos;
                }
                else
                    break;
            }
        }
    }
    list.sort(cmp_list);
}

void KVStore::compact()
{
    uint64_t levelMax = 1;
    uint32_t levelNum = cache.size();
    for (uint32_t i = 0; i < levelNum; ++i)
    {
        levelMax *= 2;
        if (cache[i].size() > levelMax)
            compactLevel(i);
        else
            break;
    }
}

void KVStore::compactLevel(uint32_t level)
{
    std::vector<range> levelRange;
    std::vector<SSTable> tableCompact;

    if (level == 0)
    {
        for (auto it = cache[level].begin(); it != cache[level].end(); ++it)
        {
            levelRange.push_back(range((*it)->Header.min, (*it)->Header.max));
            tableCompact.push_back(SSTable(*it));
        }
        cache[level].clear();
    }
    else
    {
        uint64_t mid = 1 << level;
        auto it = cache[level].begin();
        for (uint64_t i = 0; i < mid; ++i)
            ++it;
        uint64_t newTime = ((*it)->Header).timestamp;
        while (it != cache[level].begin())
        {
            if (((*it)->Header).timestamp > newTime)
                break;
            --it;
        }
        if (((*it)->Header).timestamp > newTime)
            ++it;
        while (it != cache[level].end())
        {
            levelRange.push_back(range(((*it)->Header).min, ((*it)->Header).max));
            tableCompact.push_back(SSTable(*it));
            it = cache[level].erase(it);
        }
    }
    ++level;
    if (level < cache.size())
    {
        for (auto it = cache[level].begin(); it != cache[level].end();)
        {
            if (haveIntersection(*it, levelRange))
            {
                tableCompact.push_back(SSTable(*it));
                it = cache[level].erase(it);
            }
            else
                ++it;
        }
    }
    else
    {
        utils::mkdir((dataDir + "/level-" + std::to_string(level)).c_str());
        cache.push_back(std::vector<SSTableCache *>());
    }
    for (auto it = tableCompact.begin(); it != tableCompact.end(); ++it)
        utils::rmfile((*it).path.c_str());
    sort(tableCompact.begin(), tableCompact.end(), tableTimecompare);
    SSTable::merge(tableCompact);
    std::vector<SSTableCache *> newCaches = tableCompact[0].save(dataDir + "/level-" + std::to_string(level));
    for (auto it = newCaches.begin(); it != newCaches.end(); ++it)
    {
        cache[level].push_back(*it);
    }
    std::sort(cache[level].begin(), cache[level].end(), cacheTimeCompare);
}

bool cmp_list(std::pair<uint64_t, std::string> x1, std::pair<uint64_t, std::string> x2)
{
    return x1.first <= x2.first;
}
