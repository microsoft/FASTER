// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once
#include <set>
#include <atomic>
#include <unordered_set>
#include <unordered_map>
#include "faster.h"

namespace FASTER {
namespace core {

template <class K, class V, class D>
class FasterKv;

template<typename F>
class RecordScan {
public:
  using store_t = F;
  using key_t = typename store_t::key_t;
  using value_t = typename store_t::value_t;
  using record_t = Record<key_t, typename store_t::value_t>;
  using hlog_t = typename store_t::hlog_t;
  using disk_adr_set_t = std::set<Address, std::greater<Address>>;
  
public:
  // use to hold the point of key for unordered_set
  class KeyPtr {
  public:

    KeyPtr(const key_t& key) 
      : data(&key), control(key.GetHash().control_) {}

    KeyPtr(const key_t& key, const uint64_t& control_) 
      : data(&key), control(control_) {}

    KeyPtr(const key_t* key, const uint64_t& control_) 
      : data(key), control(control_) {
    }
    
    bool operator == (const KeyPtr& other) const noexcept {
      if (data == other.data) return true;
      if (other.data == nullptr) return false;
      if (data == nullptr) return false;

      return control == other.control && *data == *other.data;
    }

    bool operator != (const KeyPtr& other) const noexcept {
      return !(*this == other);
    }

  public:
    uint64_t control{0};
    const key_t *data{nullptr};
  };

  struct KeyPtrHash {
    size_t operator()(const KeyPtr&  other) const noexcept
    {
      return other.control;
    }
  };

  class KeyAllocator {
  public:
    static const size_t kPageSize = 512 * 1024; // 512k;
    KeyAllocator(const size_t& page_size = 4) {
      pages.resize(page_size, nullptr);
      for(auto i = 0; i < page_size; i++) {
        pages[i] = (uint8_t*)malloc(kPageSize);
      }
    }
    ~KeyAllocator() {
      for(auto i = 0; i < pages.size(); i++) {
        free(pages[i]);
      }
    }

    key_t* New(const key_t& key) {
      assert(key.size() < kPageSize);
      bool more_then_page = (use_pos + key.size()) > kPageSize;
      if (more_then_page && cur_page == (pages.size() - 1)) {
        pages.push_back((uint8_t*)malloc(kPageSize));
      }
      
      if (more_then_page) {
        use_pos = key.size();
        cur_page++;
        key_t* tmp = reinterpret_cast<key_t*>(pages[cur_page]);
        memcpy(tmp, &key, key.size());
        return tmp;
      }

      key_t* tmp = reinterpret_cast<key_t*>(pages[cur_page] + use_pos);
      memcpy(tmp, &key, key.size());
      use_pos +=key.size();

      return tmp;
    }
  protected:
    size_t use_pos{0};
    size_t page_size;
    size_t cur_page{0};
    std::vector<uint8_t*> pages;
  };

  using dup_set_t = std::unordered_set<KeyPtr, KeyPtrHash>;

  // You can inherit this class for custom data traversal
  class IteratorContext {
  public:
    virtual void Iterate(record_t* record) {
      count++;
    }

    size_t& Count() {
      return count;
    }

  protected:
    size_t count{0};
  };
public:
  RecordScan() = delete;

  RecordScan(store_t *store_) 
  : store(store_) {
    assert(store_ != nullptr);
    page = reinterpret_cast<uint8_t*>(aligned_alloc(store_->hlog.sector_size,
                                                          hlog_t::kPageSize));
  }

  ~RecordScan() {
    aligned_free(reinterpret_cast<void*>(page));
  }
  
  // Traverse all available data in memory and on disk
  std::pair<size_t, size_t> TraverseRecord(IteratorContext *ictx) {
    assert(ictx != nullptr);
    uint64_t table_memory_count = 0;
    uint64_t table_disk_count = 0;
    uint64_t pre_in_disk = 0;
    disk_adr_set_t disk_addresses;
    dup_set_t dup_hash_code;
    TraverseTable(table_disk_count, table_memory_count, pre_in_disk, 
                  &disk_addresses, &dup_hash_code, ictx);

    if (disk_addresses.size() == 0) {
      return std::pair<size_t, size_t>(table_memory_count, 0);
    }

    Address head_address = store->hlog.head_address.load();
    Address begin_address = store->hlog.begin_address.load();
    Address tail_address = store->hlog.GetTailAddress();

    int it_error_count = 0;
    int disk_adr_mis = 0;
    uint64_t disk_count_new = 0;
    auto disk_current = disk_addresses.begin();
    uint32_t old_page = disk_current->page();
    uint32_t new_page = disk_current->page();

    LoadData(Address(new_page, 0));

    while(disk_current != disk_addresses.end()) {
      const Address& current = *(disk_current);
      if (new_page != old_page) {
        LoadData(Address(new_page, 0));
        old_page = new_page;
      }
      
      assert(current.offset() <= Address::kMaxOffset);
      record_t *record = GetDiskNext(current);
      assert(record != nullptr);
      Address from_address = record->header.previous_address(); 
      bool is_pre_hit = false;
      if (from_address != Address::kInvalidAddress) {
        if (from_address >= current) {
          it_error_count++;
        } else if(disk_addresses.count(from_address) == 0) {
          disk_addresses.insert(from_address);
          disk_adr_mis++; 
        }
      }

      disk_current++;
      new_page = disk_current->page();
      KeyPtr key_r(record->key());
      if (record->header.tombstone) {
        if (dup_hash_code.count(key_r) == 0) {
          dup_hash_code.insert(KeyPtr(key_allocator.New(record->key()), key_r.control));
        }
        continue;
      }

      if (dup_hash_code.count(key_r)) {
        continue;
      } else if (from_address != Address::kInvalidAddress) {
        if (dup_hash_code.count(key_r) == 0) {
          dup_hash_code.insert(KeyPtr(key_allocator.New(record->key()), key_r.control));
        }
      }

      ictx->Iterate(record);
      disk_count_new++;
    }

    size_t total_record = table_memory_count + disk_count_new;
    
    return std::pair<size_t, size_t>(total_record, it_error_count);
  }
  
  // Traverse all available data in memory or disk
  // Traverse the hash table to get the approximate number of data pieces, 
  // because some data is in the disk, the calculated size may be smaller than the actual number
  size_t NearSize() {
    assert(store != nullptr);

    uint64_t table_memory_count = 0; // record in memory
    uint64_t table_disk_count = 0; // record in disk but the address of the record is in memory
    uint64_t pre_in_disk = 0; // Number of node that its next node is on disk

    TraverseTable(table_disk_count, table_memory_count, pre_in_disk);

    return table_memory_count + table_disk_count - pre_in_disk;
  }
protected:
  class Context : public IAsyncContext {
   public:
    /// Constructs a context given a pointer to an atomic counter keeping
    /// track of the number of IOs that have completed so far.
    Context(std::atomic<bool>* done_)
     : done(done_)
    {}

    /// Destroys a Context.
    ~Context() {}

   protected:
    /// Copies this context into a passed in pointer.
    Status DeepCopy_Internal(IAsyncContext*& copy) {
      return IAsyncContext::DeepCopy_Internal(*this, copy);
    }

   public:
    /// Pointer to an atomic counter. Counts the number of IOs that have
    /// completed so far.
    std::atomic<bool>* done{nullptr};
  };

  void LoadData(const Address& begin) {
    assert(store != nullptr);
    std::atomic<bool> done{false};
    auto cb = [](IAsyncContext* ctxt, Status result, size_t bytes) {
      assert(result == Status::Ok);
      assert(bytes == hlog_t::kPageSize);

      CallbackContext<Context> context(ctxt);
      context->done->store(true);
    };

    // Issue reads to fill up the buffer and wait for them to complete.
    auto ctxt = Context(&done);
    std::memset(page, 0, hlog_t::kPageSize);

    store->hlog.file->ReadAsync(begin.control(),
                            reinterpret_cast<void*>(page),
                            hlog_t::kPageSize, cb, ctxt);
    while (!done.load()) {
      store->disk.TryComplete();
    } 
  }

  /// Returns a pointer to the next record.
  record_t* GetDiskNext(const Address& current) {
    return reinterpret_cast<record_t*>(page + current.offset());
  }

  void TraverseTable(uint64_t &table_disk_count, uint64_t& table_memory_count, uint64_t& pre_in_disk,
                  disk_adr_set_t* disk_addresses = nullptr, 
                  dup_set_t* dup_hash_code = nullptr, 
                  IteratorContext *ictx = nullptr) {
    
    uint32_t version = store->resize_info_.version;
    auto & table = store->state_[version];
    auto bucket_size = table.size();
    Address head_address = store->hlog.head_address.load();
    Address begin_address = store->hlog.begin_address.load();
    Address tail_address = store->hlog.GetTailAddress();
    for(uint64_t bucket_idx = 0; bucket_idx < bucket_size; bucket_idx++) {
      const HashBucket* bucket = & table.bucket(bucket_idx);
      while(bucket) {
        for(uint32_t entry_idx = 0; entry_idx < HashBucket::kNumEntries; ++entry_idx) {
          auto entry = bucket->entries[entry_idx].load();
          if(entry.unused()) {
            continue;
          }

          if (entry.address() < head_address) {
            if (disk_addresses != nullptr) {
              disk_addresses->insert(entry.address());
            }
            table_disk_count++;
            continue;
          } 

          record_t *record = reinterpret_cast<record_t*>(store->hlog.Get(entry.address()));
          if (record == nullptr) { 
            continue;
          }
          
          std::unordered_map<uint64_t, std::vector<const key_t*>> entry_list;
          
          auto find_or_append = [](std::vector<const key_t*>& list, const key_t* key) {
            for(const key_t * key_ptr : list) {
              if (*(key) == *(key_ptr)) {
                return true;
              }
            }
            list.push_back(key);
            return false;
          };
          const key_t* key = &record->key();
          KeyHash key_hash = key->GetHash();
          bool is_dup = find_or_append(entry_list[key_hash.control_], key);
          if (!is_dup && !record->header.tombstone) {
            table_memory_count++;
            if (ictx) {
              ictx->Iterate(record);
            }
          }

          Address from_address = record->header.previous_address(); 
          while (from_address != Address::kInvalidAddress) {
            if (dup_hash_code) {
                KeyPtr key_r(record->key(), key_hash.control_);
                if (dup_hash_code->count(key_r) == 0) {
                  dup_hash_code->insert(KeyPtr(key_allocator.New(record->key()),  key_hash.control_));
                }
            }
            if (from_address < head_address) {
              pre_in_disk++;
              table_disk_count++;
              if (disk_addresses) {
                disk_addresses->insert(from_address);
              } 
              break;
            }

            record = reinterpret_cast<record_t*>(store->hlog.Get(from_address));
            assert(record != nullptr);
            const key_t* key = &record->key();
            key_hash = key->GetHash();

            is_dup = find_or_append(entry_list[key_hash.control_], key);
            if (!is_dup && !record->header.tombstone) {       
              table_memory_count++;
              if (ictx) {
                ictx->Iterate(record);
              }
            }
            from_address = record->header.previous_address();
          }
        }

        HashBucketOverflowEntry overflow_entry = bucket->overflow_entry.load();
        if(overflow_entry.unused()) {
          bucket = nullptr;
        } else {
          bucket = &(store->overflow_buckets_allocator_[version].Get(overflow_entry.address()));
        }
      }
    }
  }
protected:
  store_t *store{nullptr};
      /// Buffer for pages that need to be scanned but are currently on disk.
  uint8_t* page;
  KeyAllocator key_allocator;
}; 

}
} // namespace FASTER::core

