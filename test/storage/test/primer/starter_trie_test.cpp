//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// starter_test.cpp
//
// Identification: test/include/starter_test.cpp
//
// Copyright (c) 2015-2020, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <bitset>
#include <functional>
#include <numeric>
#include <random>
#include <thread>  // NOLINT

#include "common/exception.h"
#include "gtest/gtest.h"
#include "primer/p0_trie.h"

namespace bustub {

// Generate n random strings
std::vector<std::string> GenerateNRandomString(int n) {
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<char> char_dist('A', 'z');
  std::uniform_int_distribution<int> len_dist(1, 30);

  std::vector<std::string> rand_strs(n);

  for (auto &rand_str : rand_strs) {
    int str_len = len_dist(gen);
    for (int i = 0; i < str_len; ++i) {
      rand_str.push_back(char_dist(gen));
    }
  }

  return rand_strs;
}

// is_end_ member var should be default initialized to false
TEST(StarterTrieNodeTest, TrieNodeConstructorTest) {
  {
    auto t = TrieNode('a');
    EXPECT_EQ(t.GetKeyChar(), 'a');
  }

  /**
   * When move constructor for TrieNode is called, the new TrieNode should
   * be able to access old node's children
   */
  {
    auto t = TrieNode('a');
    __attribute__((unused)) auto child_node = t.InsertChildNode('b', std::make_unique<TrieNode>('b'));
    child_node = t.InsertChildNode('c', std::make_unique<TrieNode>('c'));

    auto new_node = TrieNode(std::move(t));
    EXPECT_EQ(new_node.HasChild('b'), true);
    EXPECT_EQ(new_node.HasChildren(), true);
    child_node = new_node.GetChildNode('b');
    EXPECT_NE(child_node, nullptr);
    EXPECT_EQ((*child_node)->GetKeyChar(), 'b');

    EXPECT_EQ(new_node.HasChild('c'), true);
    child_node = new_node.GetChildNode('c');
    EXPECT_NE(child_node, nullptr);
    EXPECT_EQ((*child_node)->GetKeyChar(), 'c');

    new_node.RemoveChildNode('b');
    new_node.RemoveChildNode('c');
    EXPECT_EQ(new_node.HasChildren(), false);
  }
}

TEST(StarterTrieNodeTest, TrieNodeInsertRemoveTest) {
  // Test Insert
  //  When same key is inserted twice, insert should return nullptr
  // When inserted key and unique_ptr's key does not match, return nullptr
  {
    auto t = TrieNode('a');
    auto child_node = t.InsertChildNode('b', std::make_unique<TrieNode>('b'));
    EXPECT_NE(child_node, nullptr);
    EXPECT_EQ((*child_node)->GetKeyChar(), 'b');

    child_node = t.InsertChildNode('b', std::make_unique<TrieNode>('b'));
    EXPECT_EQ(child_node, nullptr);

    child_node = t.InsertChildNode('d', std::make_unique<TrieNode>('b'));
    EXPECT_EQ(child_node, nullptr);

    child_node = t.InsertChildNode('c', std::make_unique<TrieNode>('c'));
    EXPECT_EQ((*child_node)->GetKeyChar(), 'c');
  }

  // Test Remove
  {
    auto t = TrieNode('a');
    __attribute__((unused)) auto child_node = t.InsertChildNode('b', std::make_unique<TrieNode>('b'));
    child_node = t.InsertChildNode('c', std::make_unique<TrieNode>('c'));

    t.RemoveChildNode('b');
    EXPECT_EQ(t.HasChild('b'), false);
    EXPECT_EQ(t.HasChildren(), true);
    child_node = t.GetChildNode('b');
    EXPECT_EQ(child_node, nullptr);

    t.RemoveChildNode('c');
    EXPECT_EQ(t.HasChild('c'), false);
    EXPECT_EQ(t.HasChildren(), false);
    child_node = t.GetChildNode('c');
    EXPECT_EQ(child_node, nullptr);
  }
}

// Test HasChild, HasChildren, and GetChild functions
TEST(StarterTrieNodeTest, TrieNodeChildTest) {
  {
    auto t = TrieNode('a');
    auto child_node = t.InsertChildNode('b', std::make_unique<TrieNode>('b'));
    EXPECT_EQ((*child_node)->GetChildNode('c'), nullptr);
    auto tmp_child_node = t.GetChildNode('b');
    EXPECT_NE(tmp_child_node, nullptr);
    EXPECT_EQ((*tmp_child_node)->GetKeyChar(), 'b');
  }
  {
    auto t = TrieNode('a');
    EXPECT_EQ(t.HasChildren(), false);

    auto child_node = t.InsertChildNode('b', std::make_unique<TrieNode>('b'));
    EXPECT_EQ((*child_node)->GetKeyChar(), 'b');
    EXPECT_EQ((*child_node)->HasChildren(), false);
    EXPECT_EQ(t.HasChild('b'), true);
    EXPECT_EQ(t.HasChildren(), true);

    child_node = t.InsertChildNode('c', std::make_unique<TrieNode>('c'));
    EXPECT_EQ((*child_node)->GetKeyChar(), 'c');
    EXPECT_EQ((*child_node)->HasChildren(), false);
    EXPECT_EQ(t.HasChild('c'), true);
    EXPECT_EQ(t.HasChildren(), true);
  }
}

TEST(StarterTrieNodeWithValueTest, TrieNodeWithValueTest) {
  // Test constructor
  {
    auto t = TrieNodeWithValue('a', 5);
    EXPECT_EQ(t.GetKeyChar(), 'a');
    EXPECT_EQ(t.GetValue(), 5);
    __attribute__((unused)) auto child_node = t.InsertChildNode('b', std::make_unique<TrieNode>('b'));

    EXPECT_EQ(t.HasChild('b'), true);
    EXPECT_EQ(t.HasChildren(), true);

    child_node = t.GetChildNode('b');
    EXPECT_NE(child_node, nullptr);
    EXPECT_EQ((*child_node)->GetKeyChar(), 'b');
  }
  // Test move constructor
  {
    auto t = TrieNode('a');
    __attribute__((unused)) auto child_node = t.InsertChildNode('b', std::make_unique<TrieNode>('b'));
    child_node = t.InsertChildNode('c', std::make_unique<TrieNode>('c'));

    auto new_node = TrieNodeWithValue(std::move(t), 5);
    EXPECT_EQ(new_node.HasChild('b'), true);
    EXPECT_EQ(new_node.HasChildren(), true);
    child_node = new_node.GetChildNode('b');
    EXPECT_NE(child_node, nullptr);
    EXPECT_EQ((*child_node)->GetKeyChar(), 'b');

    EXPECT_EQ(new_node.HasChild('c'), true);
    child_node = new_node.GetChildNode('c');
    EXPECT_NE(child_node, nullptr);
    EXPECT_EQ((*child_node)->GetKeyChar(), 'c');

    EXPECT_EQ(new_node.GetValue(), 5);

    new_node.RemoveChildNode('b');
    new_node.RemoveChildNode('c');
    EXPECT_EQ(new_node.HasChildren(), false);
  }
}

TEST(StarterTrieTest, InsertTest) {
  {
    Trie trie;
    trie.Insert<std::string>("abc", "d");
    bool success = true;
    auto val = trie.GetValue<std::string>("abc", &success);
    EXPECT_EQ(success, true);
    EXPECT_EQ(val, "d");
  }

  // Insert empty string key should return false
  {
    Trie trie;
    auto success = trie.Insert<std::string>("", "d");
    EXPECT_EQ(success, false);
    trie.GetValue<std::string>("", &success);
    EXPECT_EQ(success, false);
  }

  // Insert duplicated key should not modify existing value
  {
    Trie trie;
    bool success = trie.Insert<int>("abc", 5);
    EXPECT_EQ(success, true);

    success = trie.Insert<int>("abc", 6);
    EXPECT_EQ(success, false);

    auto val = trie.GetValue<int>("abc", &success);
    EXPECT_EQ(success, true);
    EXPECT_EQ(val, 5);
  }

  {
    Trie trie;
    bool success = trie.Insert<int>("a", 5);
    EXPECT_EQ(success, true);
    success = trie.Insert<int>("aa", 6);
    EXPECT_EQ(success, true);
    success = trie.Insert<int>("aaa", 7);
    EXPECT_EQ(success, true);

    EXPECT_EQ(trie.GetValue<int>("a", &success), 5);
    EXPECT_EQ(success, true);
    EXPECT_EQ(trie.GetValue<int>("aa", &success), 6);
    EXPECT_EQ(success, true);
    EXPECT_EQ(trie.GetValue<int>("aaa", &success), 7);
    EXPECT_EQ(success, true);

    trie.GetValue<int>("aaaa", &success);
    EXPECT_EQ(success, false);
  }

  {
    Trie trie;
    bool success = trie.Insert<int>("aaa", 5);
    EXPECT_EQ(success, true);
    success = trie.Insert<int>("aa", 6);
    EXPECT_EQ(success, true);
    success = trie.Insert<int>("a", 7);
    EXPECT_EQ(success, true);

    trie.GetValue<int>("aaaa", &success);
    EXPECT_EQ(success, false);
    EXPECT_EQ(trie.GetValue<int>("aaa", &success), 5);
    EXPECT_EQ(success, true);
    EXPECT_EQ(trie.GetValue<int>("aa", &success), 6);
    EXPECT_EQ(success, true);
    EXPECT_EQ(trie.GetValue<int>("a", &success), 7);
    EXPECT_EQ(success, true);
  }

  {
    Trie trie;
    bool success = trie.Insert<int>("a", 5);
    EXPECT_EQ(success, true);
    success = trie.Insert<int>("ba", 6);
    EXPECT_EQ(success, true);

    trie.GetValue<int>("b", &success);
    EXPECT_EQ(success, false);
    EXPECT_EQ(trie.GetValue<int>("ba", &success), 6);
    EXPECT_EQ(success, true);
    EXPECT_EQ(trie.GetValue<int>("a", &success), 5);
    EXPECT_EQ(success, true);
  }

  // Insert different data types
  {
    Trie trie;
    constexpr int num_words = 1000;
    constexpr int num_bits = 10;
    // Use binary string as key in order to create many branches in trie
    for (int i = 0; i < num_words; ++i) {
      std::string key = std::bitset<num_bits>(i).to_string();
      bool success;
      if (i % 4 == 0) {
        success = trie.Insert<int>(key, i);
      } else if (i % 4 == 1) {
        success = trie.Insert<std::string>(key, std::to_string(i));
      } else if (i % 4 == 2) {
        success = trie.Insert<double>(key, static_cast<double>(i));
      } else {
        success = trie.Insert<char>(key, static_cast<char>(i));
      }
      EXPECT_EQ(success, true);
    }

    bool success = false;
    for (int i = 0; i < num_words; ++i) {
      std::string key = std::bitset<num_bits>(i).to_string();

      if (i % 4 == 0) {
        __attribute__((unused)) auto val1 = trie.GetValue<std::string>(key, &success);
        EXPECT_EQ(success, false);
        __attribute__((unused)) auto val2 = trie.GetValue<double>(key, &success);
        EXPECT_EQ(success, false);
        __attribute__((unused)) auto val3 = trie.GetValue<char>(key, &success);
        EXPECT_EQ(success, false);

        int val = trie.GetValue<int>(key, &success);
        EXPECT_EQ(success, true);
        EXPECT_EQ(val, i);
      } else if (i % 4 == 1) {
        __attribute__((unused)) auto val1 = trie.GetValue<int>(key, &success);
        EXPECT_EQ(success, false);
        __attribute__((unused)) auto val2 = trie.GetValue<double>(key, &success);
        EXPECT_EQ(success, false);
        __attribute__((unused)) auto val3 = trie.GetValue<char>(key, &success);
        EXPECT_EQ(success, false);

        auto val = trie.GetValue<std::string>(key, &success);
        EXPECT_EQ(success, true);
        EXPECT_EQ(val, std::to_string(i));
      } else if (i % 4 == 2) {
        __attribute__((unused)) auto val1 = trie.GetValue<std::string>(key, &success);
        EXPECT_EQ(success, false);
        __attribute__((unused)) auto val2 = trie.GetValue<int>(key, &success);
        EXPECT_EQ(success, false);
        __attribute__((unused)) auto val3 = trie.GetValue<char>(key, &success);
        EXPECT_EQ(success, false);

        auto val = trie.GetValue<double>(key, &success);
        EXPECT_EQ(success, true);
        EXPECT_EQ(val, static_cast<double>(i));
      } else {
        __attribute__((unused)) auto val1 = trie.GetValue<std::string>(key, &success);
        EXPECT_EQ(success, false);
        __attribute__((unused)) auto val2 = trie.GetValue<int>(key, &success);
        EXPECT_EQ(success, false);
        __attribute__((unused)) auto val3 = trie.GetValue<double>(key, &success);
        EXPECT_EQ(success, false);

        auto val = trie.GetValue<char>(key, &success);
        EXPECT_EQ(success, true);
        EXPECT_EQ(val, static_cast<char>(i));
      }
    }
  }
}

TEST(StarterTrieTest, RemoveTest) {
  {
    Trie trie;
    bool success = trie.Insert<int>("", 5);
    EXPECT_EQ(success, false);
  }

  {
    Trie trie;
    bool success = trie.Insert<int>("a", 5);
    EXPECT_EQ(success, true);
    success = trie.Insert<int>("aa", 6);
    EXPECT_EQ(success, true);
    success = trie.Insert<int>("aaa", 7);
    EXPECT_EQ(success, true);

    success = trie.Remove("aaa");
    EXPECT_EQ(success, true);
    trie.GetValue<int>("aaa", &success);
    EXPECT_EQ(success, false);

    success = trie.Insert("aaa", 8);
    EXPECT_EQ(success, true);
    EXPECT_EQ(trie.GetValue<int>("aaa", &success), 8);
    EXPECT_EQ(success, true);

    // Remove non-existant keys should return false
    success = trie.Remove("aaaa");
    EXPECT_EQ(success, false);

    success = trie.Remove("aa");
    EXPECT_EQ(success, true);
    success = trie.Remove("a");
    EXPECT_EQ(success, true);
    success = trie.Remove("aaa");
    EXPECT_EQ(success, true);
  }

  // Remove different data types
  {
    Trie trie;
    constexpr int num_words = 1000;
    constexpr int num_bits = 10;
    // Use binary string as key in order to create many branches in trie
    for (int i = 0; i < num_words; ++i) {
      std::string key = std::bitset<num_bits>(i).to_string();
      bool success;
      if (i % 4 == 0) {
        success = trie.Insert<int>(key, i);
      } else if (i % 4 == 1) {
        success = trie.Insert<std::string>(key, std::to_string(i));
      } else if (i % 4 == 2) {
        success = trie.Insert<double>(key, static_cast<double>(i));
      } else {
        success = trie.Insert<char>(key, static_cast<char>(i));
      }
      EXPECT_EQ(success, true);
    }

    bool success = false;
    for (int i = 0; i < num_words; ++i) {
      std::string key = std::bitset<num_bits>(i).to_string();
      success = trie.Remove(key);
      EXPECT_EQ(success, true);
    }
  }
}

TEST(StarterTrieTest, RandomElementsInsertRemoveTest) {
  Trie trie;
  int num_keys = 1000;

  // Generate 1000 random keys
  auto keys = GenerateNRandomString(num_keys);

  // Keep track of which key maps to which value
  std::map<std::string, int> key_value_store;

  // Insert all keys and store their values
  for (int i = 0; i < num_keys; ++i) {
    auto key = keys[i];
    if (key_value_store.find(key) != key_value_store.end()) {
      // If key has been inserted before, insert should return false.
      bool success = trie.Insert(key, i);
      EXPECT_EQ(success, false);
    } else {
      bool success = trie.Insert(key, i);
      EXPECT_EQ(success, true);
      key_value_store.insert({key, i});
    }
  }

  // Check all values are can be obtained
  bool success;
  for (const auto &[key, value] : key_value_store) {
    auto tval = trie.GetValue<int>(key, &success);
    EXPECT_EQ(success, true);
    EXPECT_EQ(tval, value);
  }

  // Check that all elements can be removed
  for (const auto &[key, value] : key_value_store) {
    success = trie.Remove(key);
    EXPECT_EQ(success, true);
  }

  // Trie should be empty now
  for (const auto &[key, value] : key_value_store) {
    trie.GetValue<int>(key, &success);
    EXPECT_EQ(success, false);
  }
}

TEST(StarterTrieTest, ConcurrentTest1) {
  Trie trie;
  constexpr int num_words = 1000;
  constexpr int num_bits = 10;

  std::vector<std::thread> threads;
  threads.reserve(num_words);

  auto insert_task = [&](const std::string &key, int value) {
    bool success = trie.Insert(key, value);
    EXPECT_EQ(success, true);
  };
  for (int i = 0; i < num_words; i++) {
    std::string key = std::bitset<num_bits>(i).to_string();
    threads.emplace_back(std::thread{insert_task, key, i});
  }
  for (int i = 0; i < num_words; i++) {
    threads[i].join();
  }
  threads.clear();

  auto get_task = [&](const std::string &key, int value) {
    bool success;
    int tval = trie.GetValue<int>(key, &success);
    EXPECT_EQ(success, true);
    EXPECT_EQ(tval, value);
  };
  for (int i = 0; i < num_words; i++) {
    std::string key = std::bitset<num_bits>(i).to_string();
    threads.emplace_back(std::thread{get_task, key, i});
  }
  for (int i = 0; i < num_words; i++) {
    threads[i].join();
  }
  threads.clear();

  auto remove_task = [&](const std::string &key) {
    bool success = trie.Remove(key);
    EXPECT_EQ(success, true);
    trie.GetValue<int>(key, &success);
    EXPECT_EQ(success, false);
  };
  for (int i = 0; i < num_words; i++) {
    std::string key = std::bitset<num_bits>(i).to_string();
    threads.emplace_back(std::thread{remove_task, key});
  }
  for (int i = 0; i < num_words; i++) {
    threads[i].join();
  }
  threads.clear();
}

TEST(StarterTrieTest, ConcurrentTest2) {
  Trie trie;
  int num_words = 1000;
  constexpr int num_bits = 10;

  std::vector<std::thread> threads;

  auto insert_task = [&](const std::string &key, int value) {
    bool success = trie.Insert(key, value);
    EXPECT_EQ(success, true);
  };

  auto get_task = [&](const std::string &key, int value) {
    bool success;
    int tval = trie.GetValue<int>(key, &success);
    EXPECT_EQ(success, true);
    EXPECT_EQ(tval, value);
  };

  auto remove_task = [&](const std::string &key) {
    bool success = trie.Remove(key);
    EXPECT_EQ(success, true);
    trie.GetValue<int>(key, &success);
    EXPECT_EQ(success, false);
  };

  // Insert 2/3 of the keys
  for (int i = 0; i < num_words; i++) {
    std::string key = std::bitset<num_bits>(i).to_string();
    if (i % 3 != 0) {
      threads.emplace_back(std::thread{insert_task, key, i});
    }
  }
  for (auto &thread : threads) {
    thread.join();
  }
  threads.clear();

  // Get 1/3 of the keys, remove 1/3 fo the keys, and insert 1/3 of the remaining data
  for (int i = 0; i < num_words; i++) {
    std::string key = std::bitset<num_bits>(i).to_string();
    if (i % 3 == 0) {
      threads.emplace_back(std::thread{insert_task, key, i});
    } else if (i % 3 == 1) {
      threads.emplace_back(std::thread{get_task, key, i});
    } else {
      threads.emplace_back(std::thread{remove_task, key});
    }
  }

  for (int i = 0; i < num_words; i++) {
    threads[i].join();
  }
  threads.clear();
}

}  // namespace bustub
