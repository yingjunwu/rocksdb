#include <iostream>

#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"

#include "fast_random.h"


std::string kDBPath = "/tmp/ts_example";

using namespace rocksdb;

Slice AllocateKey(std::unique_ptr<const char[]>* key_guard, const uint64_t key_size) {
  char* data = new char[key_size];
  const char* const_data = data;
  key_guard->reset(const_data);
  return Slice(key_guard->get(), key_size);
}

enum class KeyGenerateType {
  Random = 0,
  Increment,
};

void execute() {
  FastRandom fast_rand;

  Options options;
  WriteOptions write_options;
  ReadOptions read_options;

  options.create_if_missing = true;
  DB* db;
  Status s = DB::Open(options, kDBPath, &db);

  std::unique_ptr<const char[]> key_guard;
  Slice key = AllocateKey(&key_guard, 16);

  std::unique_ptr<const char[]> value_guard;
  Slice value = AllocateKey(&value_guard, 8);

  uint64_t current_ts = 0;

  for (size_t i = 0; i < 100; ++i) {
    
    // uint64_t rand_key = fast_rand.next<uint64_t>();
    uint64_t rand_key = i;

    char* key_start = const_cast<char*>(key.data());
    memcpy(key_start, (char*)(&rand_key), sizeof(uint64_t));
    memcpy(key_start + 8, (char*)(&current_ts), sizeof(uint64_t));

    uint64_t rand_value = fast_rand.next<uint64_t>();
    char* value_start = const_cast<char*>(value.data());
    memcpy(value_start, (char*)(&rand_value), sizeof(uint64_t));

    s = db->Put(write_options, key, value);

    ++current_ts;
  }

  current_ts = 0;
  for (size_t i = 0; i < 10; ++i) {
    std::string ret_value;
    uint64_t rand_key = i;

    char* key_start = const_cast<char*>(key.data());
    memcpy(key_start, (char*)(&rand_key), sizeof(uint64_t));
    memcpy(key_start + 8, (char*)(&current_ts), sizeof(uint64_t));
    s = db->Get(read_options, key, &ret_value);

    ++current_ts;
    
    std::cout << "value = " << *((const uint64_t*)(ret_value.c_str())) << std::endl;
  }
  
  // close DB
  delete db;

}

int main() {
  execute();
}
