/**
 * extendible_hash_test.cpp
 */

#include <memory>
#include <thread>  // NOLINT

#include "container/hash/extendible_hash_table.h"
#include "gtest/gtest.h"

namespace bustub {

TEST(ExtendibleHashTableTest, SampleTest) {
  auto table = std::make_unique<ExtendibleHashTable<int, std::string>>(2);

  table->Insert(1, "a");
  table->Insert(2, "b");
  table->Insert(3, "c");
  table->Insert(4, "d");
  table->Insert(5, "e");
  table->Insert(6, "f");
  table->Insert(7, "g");
  table->Insert(8, "h");
  table->Insert(9, "i");
  EXPECT_EQ(2, table->GetLocalDepth(0));
  EXPECT_EQ(3, table->GetLocalDepth(1));
  EXPECT_EQ(2, table->GetLocalDepth(2));
  EXPECT_EQ(2, table->GetLocalDepth(3));

  std::string result;
  table->Find(9, result);
  EXPECT_EQ("i", result);
  table->Find(8, result);
  EXPECT_EQ("h", result);
  table->Find(2, result);
  EXPECT_EQ("b", result);
  EXPECT_FALSE(table->Find(10, result));

  EXPECT_TRUE(table->Remove(8));
  EXPECT_TRUE(table->Remove(4));
  EXPECT_TRUE(table->Remove(1));
  EXPECT_FALSE(table->Remove(20));
}

TEST(ExtendibleHashTableTest, ConcurrentInsertTest) {
  const int num_runs = 50;
  const int num_threads = 3;

  // Run concurrent test multiple times to guarantee correctness.
  for (int run = 0; run < num_runs; run++) {
    auto table = std::make_unique<ExtendibleHashTable<int, int>>(2);
    std::vector<std::thread> threads;
    threads.reserve(num_threads);

    for (int tid = 0; tid < num_threads; tid++) {
      threads.emplace_back([tid, &table]() { table->Insert(tid, tid); });
    }
    for (int i = 0; i < num_threads; i++) {
      threads[i].join();
    }

    EXPECT_EQ(table->GetGlobalDepth(), 1);
    for (int i = 0; i < num_threads; i++) {
      int val;
      EXPECT_TRUE(table->Find(i, val));
      EXPECT_EQ(i, val);
    }
  }
}

TEST(ExtendibleHashTableTest, InsertMultipleSplitTest) {
  auto table = std::make_unique<ExtendibleHashTable<int, std::string>>(2);

  table->Insert(15, "a");
  table->Insert(14, "b");
  table->Insert(23, "c");
  table->Insert(11, "d");
  table->Insert(9, "e");

  EXPECT_EQ(4, table->GetNumBuckets());
  EXPECT_EQ(1, table->GetLocalDepth(0));
  EXPECT_EQ(2, table->GetLocalDepth(1));
  EXPECT_EQ(3, table->GetLocalDepth(3));
  EXPECT_EQ(3, table->GetLocalDepth(7));
}

TEST(ExtendibleHashTableTest, ConcurrentInsertFindTest) {
  const int num_runs = 50;
  const int num_threads = 3;

  // Run concurrent test multiple times to guarantee correctness.
  for (int run = 0; run < num_runs; run++) {
    auto table = std::make_unique<ExtendibleHashTable<int, int>>(2);
    std::vector<std::thread> threads;
    threads.reserve(num_threads);

    for (int tid = 0; tid < num_threads; tid++) {
      threads.emplace_back([tid, &table]() {
        int val;
        table->Insert(tid, tid);
        EXPECT_TRUE(table->Find(tid, val));
      });
    }
    for (int i = 0; i < num_threads; i++) {
      threads[i].join();
    }

    EXPECT_EQ(table->GetGlobalDepth(), 1);
    for (int i = 0; i < num_threads; i++) {
      int val;
      EXPECT_TRUE(table->Find(i, val));
      EXPECT_EQ(i, val);
    }
  }
}

TEST(ExtendibleHashTableTest, ConcurrentInsertFind2Test) {
  const int num_runs = 30;
  const int num_threads = 5;

  // Run concurrent test multiple times to guarantee correctness.
  for (int run = 0; run < num_runs; run++) {
    auto table = std::make_unique<ExtendibleHashTable<int, int>>(2);
    std::vector<std::thread> threadsInsert;
    std::vector<std::thread> threadsFind;
    threadsInsert.reserve(num_threads);
    threadsFind.reserve(num_threads);

    for (int tid = 0; tid < num_threads; tid++) {
      threadsInsert.emplace_back([tid, &table]() {
        for (int i = tid * 10; i < (tid + 1) * 10; i++) {
          table->Insert(i, i);
        }
      });
    }
    for (int i = 0; i < num_threads; i++) {
      threadsInsert[i].join();
    }
    for (int tid = 0; tid < num_threads; tid++) {
      threadsFind.emplace_back([tid, &table]() {
        for (int i = tid * 10; i < (tid + 1) * 10; i++) {
          int val;
          EXPECT_TRUE(table->Find(i, val));
        }
      });
    }
    for (int i = 0; i < num_threads; i++) {
      threadsFind[i].join();
    }
  }
}

TEST(ExtendibleHashTableTest, GetNumBucketsTest) {
  auto table = std::make_unique<ExtendibleHashTable<int, std::string>>(2);

  table->Insert(4, "a");
  table->Insert(12, "b");
  table->Insert(16, "c");
  EXPECT_EQ(4, table->GetNumBuckets());
  table->Insert(64, "d");
  table->Insert(31, "e");

  table->Insert(10, "f");
  table->Insert(51, "g");
  EXPECT_EQ(4, table->GetNumBuckets());
  table->Insert(15, "h");
  table->Insert(18, "i");
  table->Insert(20, "j");
  EXPECT_EQ(7, table->GetNumBuckets());
  table->Insert(7, "k");
  table->Insert(23, "l");

  EXPECT_EQ(8, table->GetNumBuckets());
}

}  // namespace bustub
