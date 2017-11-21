// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/utilities/transaction.h"
#include "rocksdb/utilities/transaction_db.h"

#include "rocksdb/fast_random.h"
#include "rocksdb/time_measurer.h"

#include <getopt.h>
#include <iostream>
#include <vector>
#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <thread>

using namespace rocksdb;

struct YcsbConfig {

  const size_t default_table_size_ = 1000;
  
  size_t thread_count_; // number of threads
  
  float scale_factor_; // scale factor
  
  float zipf_theta_; // skewness
  
  size_t operation_count_; // number of operations in a transaction

  float update_ratio_; // update ratio

  float duration_; // time duration (s)

};

void Usage(FILE *out) {
  fprintf(out,
          "Command line options : ycsb_benchmark <options> \n"
          "   -h --help              :  print help message \n"
          "   -t --thread_count      :  # of threads \n"
          "   -k --scale_factor      :  scale factor \n"
          "   -z --zipf_theta        :  zipf theta \n"
          "   -o --operation_count   :  # of operations \n"
          "   -u --update_ratio      :  update ratio \n"
          "   -d --duration          :  total processing time duration \n"
  );
}

static struct option opts[] = {
    { "thread_count", optional_argument, NULL, 't' },
    { "scale_factor", optional_argument, NULL, 'k' },
    { "zipf_theta", optional_argument, NULL, 'z' },
    { "operation_count", optional_argument, NULL, 'o' },
    { "update_ratio", optional_argument, NULL, 'u' },
    { "duration", optional_argument, NULL, 'd' },
    { NULL, 0, NULL, 0 }
};

void ParseArguments(int argc, char *argv[], YcsbConfig &conf) {

  conf.thread_count_ = 1;
  conf.scale_factor_ = 1;
  conf.zipf_theta_ = 0;
  conf.operation_count_ = 1;
  conf.update_ratio_ = 0;
  conf.duration_ = 10;
  
  // parse args
  while (1) {
    int idx = 0;
    int c = getopt_long(argc, argv, "ht:k:z:o:u:d:", opts, &idx);

    if (c == -1) break;

    switch(c) {
      case 't':
        conf.thread_count_ = atoi(optarg);
        break;
      case 'k':
        conf.scale_factor_ = atof(optarg);
        break;
      case 'z':
        conf.zipf_theta_ = atof(optarg);
        break;
      case 'o':
        conf.operation_count_ = atoi(optarg);
        break;
      case 'u':
        conf.update_ratio_ = atof(optarg);
        break;
      case 'd':
        conf.duration_ = atof(optarg);
        break;
      case 'h':
      Usage(stderr);
      exit(EXIT_FAILURE);
      break;

      default:
      Usage(stderr);
      exit(EXIT_FAILURE);

    }
  }
}

//////////////////////////////////////////////////////////////////////

void Populate(TransactionDB* txn_db, const YcsbConfig &config) {

  size_t table_size = config.default_table_size_ * config.scale_factor_;
  
  std::cout << std::endl
            << ">>>>> Populate table. " << std::endl
            << "   -- Table size   : " << table_size << std::endl;

  WriteOptions write_options;
  TransactionOptions txn_options;

  // Start a transaction
  Transaction* txn = txn_db->BeginTransaction(write_options);
  assert(txn);

  Status s;

  for (size_t i = 0; i < table_size; ++i) {
    s = txn->Put(std::to_string(i), "a");
    assert(s.ok());
  }

  // Commit transaction
  s = txn->Commit();
  assert(s.ok());
  delete txn;
  txn = nullptr;
}

volatile bool is_running = true;
size_t *commit_counts;

void ProcessThread(TransactionDB* txn_db, const YcsbConfig &config, const size_t thread_id) {
  
  size_t &commit_count_ref = commit_counts[thread_id];

  size_t table_size = config.default_table_size_ * config.scale_factor_;

  FastRandom fast_rand;

  ZipfDistribution zipf(table_size, config.zipf_theta_);

  WriteOptions write_options;
  ReadOptions read_options;
  TransactionOptions txn_options;
  std::string value;
  
  while (true) {
    if (is_running == false) {
      break;
    }
    // Start a transaction
    Transaction* txn = txn_db->BeginTransaction(write_options);
    assert(txn);

    Status s;

    for (size_t i = 0; i < config.operation_count_; ++i) {
      
      size_t key = zipf.GetNextNumber() - 1;
      
      if (fast_rand.next_uniform() < config.update_ratio_) {
        // update
        s = txn->Put(std::to_string(key), "z");
        assert(s.ok());
      } else {
        // select
        s = txn_db->Get(read_options, std::to_string(key), &value);
        // std::cout << "value = " << value << std::endl;
        assert(s.ok());
      }
    }

    // Commit transaction
    s = txn->Commit();
    assert(s.ok());
    delete txn;
    txn = nullptr;
    commit_count_ref++;
  }

}

void Process(TransactionDB* txn_db, const YcsbConfig &config) {
  
  size_t table_size = config.default_table_size_ * config.scale_factor_;

  std::cout << std::endl
            << ">>>>> Process transactions. " << std::endl
            << "   -- Table size               : " << table_size << std::endl
            << "   -- Operation count          : " << config.operation_count_ << std::endl
            << "   -- Update ratio             : " << config.update_ratio_ << std::endl
            << "   -- Zipf theta               : " << config.zipf_theta_ << std::endl
            << "   -- Duration                 : " << config.duration_ << std::endl;


  size_t thread_count = config.thread_count_;

  commit_counts = new size_t[thread_count];
  memset(commit_counts, 0, sizeof(size_t) * thread_count);

  std::vector<std::thread> thread_group;

  for (size_t i = 0; i < thread_count; ++i) {
    thread_group.push_back(
      std::move(std::thread(ProcessThread, txn_db, std::ref(config), i)));
  }
  
  std::this_thread::sleep_for(
      std::chrono::milliseconds(int(config.duration_ * 1000)));

  is_running = false;

  for (size_t i = 0; i < thread_count; ++i) {
    thread_group[i].join();
  }

  size_t total_commit_count = 0;
  for (size_t i = 0; i < thread_count; ++i) {
    total_commit_count += commit_counts[i];
  }

  float tps = total_commit_count / config.duration_ / 1000;
  std::cout << std::endl 
            << ">>>>> Results. " << std::endl
            << "   -- TPS             : " << tps << " K txn/s" << std::endl
            << "   -- Per-thread TPS  : " << tps / config.thread_count_ << " K txn/s" << std::endl;

  delete[] commit_counts;
  commit_counts = nullptr;
}

//////////////////////////////////////////////////////////////////////


std::string kDBPath = "/tmp/rocksdb_transaction_example";

int main(int argc, char **argv) {

  srand(time(NULL));
  
  YcsbConfig config;

  ParseArguments(argc, argv, config);

  std::cout << "run ycsb benchmark!" << std::endl;

  // open DB
  Options options;
  TransactionDBOptions txn_db_options;
  options.create_if_missing = true;
  TransactionDB* txn_db;

  Status s = TransactionDB::Open(options, txn_db_options, kDBPath, &txn_db);
  assert(s.ok());

  Populate(txn_db, config);

  Process(txn_db, config);

  // Cleanup
  delete txn_db;
  DestroyDB(kDBPath, options);

  std::cout << std::endl;

  return 0;
}
