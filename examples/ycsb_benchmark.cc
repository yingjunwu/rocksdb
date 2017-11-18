// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE

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

using namespace rocksdb;

struct YcsbConfig {

  const size_t default_table_size_ = 1000;
  
  size_t thread_count_;
  
  float scale_factor_;
  
  float zipf_theta_;
  
  // operation count in a transaction
  size_t operation_count_;

  // update ratio
  float update_ratio_;

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
  );
}

static struct option opts[] = {
    { "thread_count", optional_argument, NULL, 't' },
    { "scale_factor", optional_argument, NULL, 'k' },
    { "zipf_theta", optional_argument, NULL, 'z' },
    { "operation_count", optional_argument, NULL, 'o' },
    { "update_ratio", optional_argument, NULL, 'u' },
    { NULL, 0, NULL, 0 }
};

void ParseArguments(int argc, char *argv[], YcsbConfig &conf) {

  conf.thread_count_ = 1;
  conf.scale_factor_ = 1;
  conf.zipf_theta_ = 0;
  conf.operation_count_ = 1;
  conf.update_ratio_ = 0;
  
  // parse args
  while (1) {
    int idx = 0;
    int c = getopt_long(argc, argv, "ht:k:z:o:u:", opts, &idx);

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
  
  std::cout << ">>>>> Populate table. " << std::endl
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

void ProcessTransaction(TransactionDB* txn_db, const YcsbConfig &config) {
  
  srand(time(NULL));

  size_t table_size = config.default_table_size_ * config.scale_factor_;

  FastRandom fast_rand;

  ZipfDistribution zipf(table_size, config.zipf_theta_);

  WriteOptions write_options;
  ReadOptions read_options;
  TransactionOptions txn_options;
  std::string value;

  std::cout << ">>>>> Process transactions. " << std::endl
            << "   -- Table size               : " << table_size << std::endl
            << "   -- Operation count          : " << config.operation_count_ << std::endl
            << "   -- Update ratio             : " << config.update_ratio_ << std::endl
            << "   -- Zipf theta               : " << config.zipf_theta_ << std::endl;
  

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
      std::cout << "value = " << value << std::endl;
      assert(s.ok());      
    }
  }

  // Commit transaction
  s = txn->Commit();
  assert(s.ok());
  delete txn;
  txn = nullptr;
}

//////////////////////////////////////////////////////////////////////


std::string kDBPath = "/tmp/rocksdb_transaction_example";

int main(int argc, char **argv) {
  
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

  ProcessTransaction(txn_db, config);

  // Cleanup
  delete txn_db;
  DestroyDB(kDBPath, options);
  return 0;
}

#endif  // ROCKSDB_LITE
