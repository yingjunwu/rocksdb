#include <cstdio>
#include <string>
#include <iostream>

#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"
#include "rocksdb/time_measurer.h"

using namespace rocksdb;

std::string kDBPath = "/tmp/rocksdb_simple_benchmark";

int main() {
  DB* db;
  Options options;
  // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
  options.IncreaseParallelism();
  options.OptimizeLevelStyleCompaction();
  // create the DB if it's not already present
  options.create_if_missing = true;

  // open DB
  Status s = DB::Open(options, kDBPath, &db);
  assert(s.ok());


  TimeMeasurer timer;

  timer.StartTimer();

  for (size_t i = 0; i < 5; ++i) {
    std::string key_str = "key" + std::to_string(i);
    // Put key-value
    s = db->Put(WriteOptions(), key_str, "value");
    assert(s.ok());
  }

  timer.EndTimer();
  std::cout << "elapsed time = " << timer.GetElapsedMilliSeconds() << " ms" << std::endl;

  std::string value;
  // get value
  s = db->Get(ReadOptions(), "key1", &value);
  assert(s.ok());
  assert(value == "value");

  delete db;

  return 0;
}