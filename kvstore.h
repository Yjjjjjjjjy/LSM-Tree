#pragma once

#include "kvstore_api.h"
#include "skiplist.h"
#include <vector>

class KVStore : public KVStoreAPI
{
	// You can add your implementation here
private:
//	SkipList *memTable;
	std::vector<std::vector<SSTableCache *>> cache;
	unsigned long long currentTime;
	std::string dataDir;

	void compact();
    void compactLevel(uint32_t level);

public:
    SkipList *memTable;
	KVStore(const std::string &dir);

	~KVStore();

	void put(uint64_t key, const std::string &s) override;

	std::string get(uint64_t key) override;

	bool del(uint64_t key) override;

	void reset() override;

	void scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string>> &list) override;
};

bool cmp_list(std::pair<uint64_t, std::string> x1, std::pair<uint64_t, std::string> x2);
