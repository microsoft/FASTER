// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include "address.h"
#include "async.h"
#include "key_hash.h"
#include "guid.h"
#include "record.h"

#include "../index/hash_bucket.h"

using namespace FASTER::index;

namespace FASTER {
namespace core {

// A helper class to copy the key into FASTER log.
// In old API, the Key provided is just the Key type, and we use in-place-new and copy constructor
// to copy the key into the log. In new API, the user provides a ShallowKey, and we call the
// ShallowKey's write_deep_key_at() method to write the key content into the log.
// New API case (user provides ShallowKey)
//
template<bool isShallowKey>
struct write_deep_key_at_helper
{
  template<class ShallowKey, class Key>
  static inline void execute(const ShallowKey& key, Key* dst) {
    key.write_deep_key_at(dst);
  }
};
// Old API case (user provides Key)
//
template<>
struct write_deep_key_at_helper<false>
{
  template<class Key>
  static inline void execute(const Key& key, Key* dst) {
    new (dst) Key(key);
  }
};


template<bool isF2Context>
struct f2_context_helper
{
  /// Used by all FASTER pending contexts
  template<class RC>
  static inline uint32_t key_size(RC& context) {
    return context.key_size();
  }
  template<class RC>
  static inline KeyHash get_key_hash(RC& context) {
    return context.get_key_hash();
  }
  template<class RC, class Key>
  static inline bool is_key_equal(RC& context, const Key& other) {
    return context.is_key_equal(other);
  }
  template<class MC, class Key>
  static void write_deep_key_at(MC& context, Key* dst) {
    context.write_deep_key_at(dst);
  }

  /// Used by FASTER's Read pending context
  template<class RC, class Record>
  static inline void Get(RC& context, const void* rec) {
    context.Get(rec);
  }
  template<class RC, class Record>
  static inline void GetAtomic(RC& context, const void* rec) {
    context.GetAtomic(rec);
  }
  /// Used by FASTER's Upsert pending context
  template<class UC, class Record>
  static inline void Put(UC& context, void* rec) {
    context.Put(rec);
  }
  template<class UC, class Record>
  static inline bool PutAtomic(UC& context, void* rec) {
    return context.PutAtomic(rec);
  }
};

template<>
struct f2_context_helper<false>
{
  /// Used by user-provided contexts
  template<class RC>
  static inline uint32_t key_size(RC& context) {
    return context.key().size();
  }
  template<class RC>
  static inline KeyHash get_key_hash(RC& context) {
    return context.key().GetHash();
  }
  template<class RC, class Key>
  static inline bool is_key_equal(RC& context, const Key& other) {
    return context.key() == other;
  }
  template<class MC, class Key>
  static inline void write_deep_key_at(MC& context, Key* dst) {
    using key_or_shallow_key_t = std::remove_const_t<std::remove_reference_t<std::invoke_result_t<decltype(&MC::key), MC>>>;
    constexpr static const bool kIsShallowKey = !std::is_same<key_or_shallow_key_t, Key>::value;

    write_deep_key_at_helper<kIsShallowKey>::execute(context.key(), dst);
  }

  /// Used by FASTER's Read pending context
  template<class RC, class Record>
  static inline void Get(RC& context, const void* rec) {
    const Record* record = reinterpret_cast<const Record*>(rec);
    context.Get(record->value());
  }
  template<class RC, class Record>
  static inline void GetAtomic(RC& context, const void* rec) {
    const Record* record = reinterpret_cast<const Record*>(rec);
    context.GetAtomic(record->value());
  }
  /// Used by FASTER's Upsert pending context
  template<class UC, class Record>
  static inline void Put(UC& context, void* rec) {
    Record* record = reinterpret_cast<Record*>(rec);
    context.Put(record->value());
  }
  template<class UC, class Record>
  static inline bool PutAtomic(UC& context, void* rec) {
    Record* record = reinterpret_cast<Record*>(rec);
    return context.PutAtomic(record->value());
  }
};

/// Internal contexts, used by F2
enum class ReadOperationStage {
  HOT_LOG_READ = 1,
  COLD_LOG_READ = 2,
};

enum class RmwOperationStage {
  HOT_LOG_RMW = 1,
  COLD_LOG_READ = 2,
  HOT_LOG_CONDITIONAL_INSERT = 3,
  WAIT_FOR_RETRY = 4,
};

template <class K>
class F2Context : public IAsyncContext {
 public:
  typedef K key_t;

  F2Context(void* f2_, IAsyncContext& caller_context_,
            AsyncCallback caller_callback_, uint64_t monotonic_serial_num_)
    : f2{ f2_ }
    , caller_context{ &caller_context_ }
    , caller_callback{ caller_callback_ }
    , serial_num{ monotonic_serial_num_ }
  {}
  /// No copy constructor.
  F2Context(const F2Context& other) = delete;
  /// The deep-copy constructor.
  F2Context(F2Context& other, IAsyncContext* caller_context_)
    : f2{ other.f2 }
    , caller_context{ caller_context_ }
    , caller_callback{ other.caller_callback }
    , serial_num{ other.serial_num }
  {}

  /// Points to F2
  void* f2;
  /// User context
  IAsyncContext* caller_context;
  /// User-provided callback
  AsyncCallback caller_callback;
  /// Request serial num
  uint64_t serial_num;
};

/// Context that holds user context for Read request
template <class K, class V>
class AsyncF2ReadContext : public F2Context<K> {
 public:
  typedef K key_t;
  typedef V value_t;

 protected:
  AsyncF2ReadContext(void* f2_, ReadOperationStage stage_, Address orig_hlog_tail_address_,
                     IAsyncContext& caller_context_, AsyncCallback caller_callback_,
                     uint64_t monotonic_serial_num_)
    : F2Context<key_t>(f2_, caller_context_, caller_callback_, monotonic_serial_num_)
    , stage{ stage_ }
    , hot_index_expected_entry{ HashBucketEntry::kInvalidEntry }
    , orig_hlog_tail_address{ orig_hlog_tail_address_ }
    , record{ nullptr }
  {}
 public:
  virtual ~AsyncF2ReadContext() {
    if (record) {
      delete[] record;
    }
  }
 protected:
  /// The deep-copy constructor.
  AsyncF2ReadContext(AsyncF2ReadContext& other_, IAsyncContext* caller_context_)
    : F2Context<key_t>(other_, caller_context_)
    , stage{ other_.stage }
    , hot_index_expected_entry{ other_.hot_index_expected_entry }
    , orig_hlog_tail_address{ other_.orig_hlog_tail_address }
    , record{ other_.record }
  {}
 public:
  virtual uint32_t key_size() const = 0;
  virtual KeyHash get_key_hash() const = 0;
  virtual bool is_key_equal(const key_t& other) const = 0;

  virtual void Get(const void* rec) = 0;
  virtual void GetAtomic(const void* rec) = 0;

  ReadOperationStage stage;
  HashBucketEntry hot_index_expected_entry;
  Address orig_hlog_tail_address;

  uint8_t* record;
};

/// Context that holds user context for Read request
template <class RC>
class F2ReadContext : public AsyncF2ReadContext<typename RC::key_t, typename RC::value_t> {
 public:
  typedef RC read_context_t;
  typedef typename read_context_t::key_t key_t;
  typedef typename read_context_t::value_t value_t;
  typedef Record<key_t, value_t> record_t;

  F2ReadContext(void* f2_, ReadOperationStage stage_, Address orig_hlog_tail_address_,
                read_context_t& caller_context_, AsyncCallback caller_callback_,
                uint64_t monotonic_serial_num_)
    : AsyncF2ReadContext<key_t, value_t>(f2_, stage_, orig_hlog_tail_address_, caller_context_,
                                         caller_callback_, monotonic_serial_num_)
    {}

  /// The deep-copy constructor.
  F2ReadContext(F2ReadContext& other_, IAsyncContext* caller_context_)
    : AsyncF2ReadContext<key_t, value_t>(other_, caller_context_)
    {}

 protected:
  Status DeepCopy_Internal(IAsyncContext*& context_copy) final {
    return IAsyncContext::DeepCopy_Internal(*this, F2Context<key_t>::caller_context,
                                            context_copy);
  }
 private:
  inline const read_context_t& read_context() const {
    return *static_cast<const read_context_t*>(F2Context<key_t>::caller_context);
  }
  inline read_context_t& read_context() {
    return *static_cast<read_context_t*>(F2Context<key_t>::caller_context);
  }
  inline void copy_if_cold_log_record(const void* rec) {
    if (this->stage == ReadOperationStage::COLD_LOG_READ) {
      // Copy record to this context to be inserted later to Read Cache
      // TODO: We can optimize this by using SectorAlignedMemory to "move" the
      //       record from the AsyncIOContext to here and avoid copying
      const record_t* record_ = reinterpret_cast<const record_t*>(rec);
      this->record = new uint8_t[record_->size()];
      memcpy(this->record, record_, record_->size());
    }
  }
 public:
  /// Propagates calls to caller context
  inline uint32_t key_size() const final {
    return f2_context_helper<false>::key_size(read_context());
  }
  inline KeyHash get_key_hash() const final {
    return f2_context_helper<false>::get_key_hash(read_context());
  }
  inline bool is_key_equal(const key_t& other) const final {
    return f2_context_helper<false>::is_key_equal(read_context(), other);
  }
  inline void Get(const void* rec) {
    copy_if_cold_log_record(rec);
    return f2_context_helper<false>::template Get<read_context_t, record_t>(read_context(), rec);
  }
  inline void GetAtomic(const void* rec) {
    copy_if_cold_log_record(rec);
    return f2_context_helper<false>::template GetAtomic<read_context_t, record_t>(read_context(), rec);
  }

};

// Forward class declarations
template <class K, class V>
class F2RmwReadContext;

template <class K, class V>
class F2RmwConditionalInsertContext;

/// Context that holds user context for RMW request
template <class K, class V>
class AsyncF2RmwContext : public F2Context<K> {
 public:
  typedef K key_t;
  typedef V value_t;
  typedef F2RmwReadContext<K, V> f2_rmw_read_context_t;
  typedef F2RmwConditionalInsertContext<K, V> f2_rmw_ci_context_t;

 protected:
  AsyncF2RmwContext(void* f2_, RmwOperationStage stage_, Address expected_hlog_address_,
                    Address orig_hlog_tail_address_, IAsyncContext& caller_context_,
                    AsyncCallback caller_callback_, uint64_t monotonic_serial_num_)
    : F2Context<key_t>(f2_, caller_context_, caller_callback_, monotonic_serial_num_)
    , stage{ stage_ }
    , expected_hlog_address{ expected_hlog_address_ }
    , orig_hlog_tail_address{ orig_hlog_tail_address_ }
    , read_context{ nullptr }
    , ci_context{ nullptr }
  {}
  /// The deep copy constructor.
  AsyncF2RmwContext(AsyncF2RmwContext& other, IAsyncContext* caller_context)
    : F2Context<key_t>(other, caller_context)
    , stage{ other.stage }
    , expected_hlog_address{ other.expected_hlog_address }
    , orig_hlog_tail_address{ other.orig_hlog_tail_address }
    , read_context{ other.read_context }
    , ci_context{ other.ci_context }
  {}
 public:
  //virtual const key_t& key() const = 0;
  virtual uint32_t key_size() const = 0;
  virtual void write_deep_key_at(key_t* dst) const = 0;
  virtual KeyHash get_key_hash() const = 0;
  virtual bool is_key_equal(const key_t& other) const = 0;

  /// Set initial value.
  virtual void RmwInitial(value_t& value) = 0;
  /// RCU.
  virtual void RmwCopy(const value_t& old_value, value_t& value) = 0;
  /// in-place update.
  virtual bool RmwAtomic(value_t& value) = 0;
  /// Get value size for initial value or in-place update
  virtual uint32_t value_size() const = 0;
  /// Get value size for RCU
  virtual uint32_t value_size(const value_t& value) const = 0;

  void prepare_for_retry() {
    stage = RmwOperationStage::WAIT_FOR_RETRY;
    expected_hlog_address = Address::kInvalidAddress;
    this->free_aux_contexts();
  }

  void free_aux_contexts() {
    if (read_context != nullptr && read_context->from_deep_copy()) {
      // free context
      context_unique_ptr_t<f2_rmw_read_context_t> context =
          make_context_unique_ptr(static_cast<f2_rmw_read_context_t*>(read_context));
    }
    if (ci_context != nullptr && ci_context->from_deep_copy()) {
      // free context
      context_unique_ptr_t<f2_rmw_ci_context_t> context =
          make_context_unique_ptr(static_cast<f2_rmw_ci_context_t*>(ci_context));
    }
    read_context = ci_context = nullptr;
  }

  RmwOperationStage stage;
  Address expected_hlog_address;
  Address orig_hlog_tail_address;

  IAsyncContext* read_context;  // F2RmwRead context
  IAsyncContext* ci_context;    // F2RmwConditionalInsert context
};

template <class MC>
class F2RmwContext : public AsyncF2RmwContext<typename MC::key_t, typename MC::value_t> {
 public:
  typedef MC rmw_context_t;
  typedef typename rmw_context_t::key_t key_t;
  typedef typename rmw_context_t::value_t value_t;
  typedef Record<key_t, value_t> record_t;

  F2RmwContext(void* f2_, RmwOperationStage stage_, Address expected_hlog_address_,
               Address orig_hlog_tail_address_, rmw_context_t& caller_context_,
               AsyncCallback caller_callback_, uint64_t monotonic_serial_num_)
    : AsyncF2RmwContext<key_t, value_t>(f2_, stage_, expected_hlog_address_, orig_hlog_tail_address_,
                                        caller_context_, caller_callback_, monotonic_serial_num_)
    {}
  /// The deep-copy constructor.
  F2RmwContext(F2RmwContext& other_, IAsyncContext* caller_context_)
    : AsyncF2RmwContext<key_t, value_t>(other_, caller_context_)
    {}

 protected:
  Status DeepCopy_Internal(IAsyncContext*& context_copy) final {
    // Deep copy this, and the user RMW context
    return IAsyncContext::DeepCopy_Internal(
        *this, F2Context<key_t>::caller_context, context_copy);
  }
 private:
  inline const rmw_context_t& rmw_context() const {
    return *static_cast<const rmw_context_t*>(F2Context<key_t>::caller_context);
  }
  inline rmw_context_t& rmw_context() {
    return *static_cast<rmw_context_t*>(F2Context<key_t>::caller_context);
  }
 public:
  inline uint32_t key_size() const final {
    return f2_context_helper<false>::key_size(rmw_context());
  }
  inline KeyHash get_key_hash() const final {
    return f2_context_helper<false>::get_key_hash(rmw_context());
  }
  inline bool is_key_equal(const key_t& other) const final {
    return f2_context_helper<false>::is_key_equal(rmw_context(), other);
  }
  inline void write_deep_key_at(key_t* dst) const {
    f2_context_helper<false>::write_deep_key_at(rmw_context(), dst);
  }

  /// Set initial value.
  inline void RmwInitial(value_t& value) final {
    rmw_context().RmwInitial(value);
  }
  /// RCU.
  inline void RmwCopy(const value_t& old_value, value_t& value) final {
    rmw_context().RmwCopy(old_value, value);
  }
  /// in-place update.
  inline bool RmwAtomic(value_t& value) final {
    return rmw_context().RmwAtomic(value);
  }
  /// Get value size for initial value or in-place update
  inline constexpr uint32_t value_size() const final {
    return rmw_context().value_size();
  }
  /// Get value size for RCU
  inline constexpr uint32_t value_size(const value_t& value) const final {
    return rmw_context().value_size(value);
  }
};

/// Internal context that holds the F2 RMW context when doing Read
template<class K, class V>
class F2RmwReadContext : public IAsyncContext {
 public:
  typedef K key_t;
  typedef V value_t;
  typedef Record<key_t, value_t> record_t;
  typedef AsyncF2RmwContext<K, V> f2_rmw_context_t;

  F2RmwReadContext(IAsyncContext* f2_rmw_context_)
    : f2_rmw_context{ f2_rmw_context_ }
    , record{ nullptr }
    , deep_copied_{ false }
    {}

  /// The deep-copy constructor.
  F2RmwReadContext(F2RmwReadContext& other)
    : f2_rmw_context{ other.f2_rmw_context }
    , record{ other.record }
    , deep_copied_{ false }
    {}

  ~F2RmwReadContext() {
    if (record != nullptr && (!deep_copied_ || this->from_deep_copy())) {
      delete[] record;
    }
  }

 protected:
  Status DeepCopy_Internal(IAsyncContext*& context_copy) final {
    typedef AsyncF2RmwContext<key_t, value_t> async_f2_rmw_context_t;

    // Deep copy F2 RMW context
    IAsyncContext* rmw_context_copy;
    auto rmw_context = static_cast<async_f2_rmw_context_t*>(this->f2_rmw_context);
    RETURN_NOT_OK(
      rmw_context->DeepCopy(rmw_context_copy));
    this->f2_rmw_context = rmw_context_copy;
    rmw_context = static_cast<async_f2_rmw_context_t*>(this->f2_rmw_context);

    // Deep copy this context
    RETURN_NOT_OK(
      IAsyncContext::DeepCopy_Internal(*this, context_copy));
    rmw_context->read_context = context_copy;
    deep_copied_ = true;

    return Status::Ok;
  }

 private:
  inline const f2_rmw_context_t& rmw_context() const {
    return *static_cast<const f2_rmw_context_t*>(f2_rmw_context);
  }
  inline f2_rmw_context_t& rmw_context() {
    return *static_cast<f2_rmw_context_t*>(f2_rmw_context);
  }
 public:
  /// Propagates calls to caller context
  inline uint32_t key_size() const {
    return f2_context_helper<true>::key_size(rmw_context());
  }
  inline KeyHash get_key_hash() const {
    return f2_context_helper<true>::get_key_hash(rmw_context());
  }
  inline bool is_key_equal(const key_t& other) const {
    return f2_context_helper<true>::is_key_equal(rmw_context(), other);
  }

 public:
  inline const value_t* value() const {
    assert(this->record != nullptr);
    record_t* record_ = reinterpret_cast<record_t*>(record);
    return &record_->value();
  }

  inline void Get(const void* rec) {
    record_t* record_ = const_cast<record_t*>(reinterpret_cast<const record_t*>(rec));
    assert(record_ != nullptr);
    // TODO: avoid copying (if possible)
    this->record = new uint8_t[record_->size()];
    memcpy(this->record, record_, record_->size());
  }
  inline void GetAtomic(const void* rec) {
    // TODO: check if this is fine.
    // There might be a race condition when hot-cold compaction is copying record and
    // another thread copies the record to the hot log as part of RMW op -- need to check though.
    this->Get(rec);
  }

  IAsyncContext* f2_rmw_context;
  uint8_t* record;

 private:
  bool deep_copied_;
};

/// Context used for F2 sync hash index operations
/// Currently used by F2's Read() and RMW() ops
class F2IndexContext : public IAsyncContext {
 public:
  /// Constructs and returns a context given a F2 context
  F2IndexContext(KeyHash key_hash)
    : key_hash_{ key_hash }
    , entry{ HashBucketEntry::kInvalidEntry }
    , atomic_entry{ nullptr } {
  }
  /// Copy constructor deleted -- op does not go async
  F2IndexContext(const F2IndexContext& from) = delete;

  inline KeyHash get_key_hash() const {
    return key_hash_;
  }
  inline void set_index_entry(HashBucketEntry entry_, AtomicHashBucketEntry* atomic_entry_) {
    entry = entry_;
    atomic_entry = atomic_entry_;
  }

 protected:
  /// Copies this context into a passed-in pointer if the operation goes
  /// asynchronous inside FASTER.
  Status DeepCopy_Internal(IAsyncContext*& context_copy) {
    throw std::runtime_error{ "F2IndexContext should not go async!" };
  }

 private:
  KeyHash key_hash_;

 public:
  HashBucketEntry entry;
  AtomicHashBucketEntry* atomic_entry;
};


template <class RC>
constexpr bool is_f2_read_context =
  std::integral_constant<bool,
      std::is_base_of<AsyncF2ReadContext<typename RC::key_t, typename RC::value_t>, RC>::value ||
      std::is_base_of<F2RmwReadContext<typename RC::key_t, typename RC::value_t>, RC>::value>::value;

template <class MC>
constexpr bool is_f2_rmw_context = std::is_base_of<AsyncF2RmwContext<typename MC::key_t, typename MC::value_t>, MC>::value;


/// Internal context that holds the F2 RMW context when doing ConditionalInsert
template <class K, class V>
class F2RmwConditionalInsertContext: public IAsyncContext {
 public:
  // Typedefs on the key and value required internally by FASTER.
  typedef K key_t;
  typedef V value_t;
  typedef AsyncF2RmwContext<key_t, value_t> rmw_context_t;
  typedef F2RmwReadContext<key_t, value_t> rmw_read_context_t;
  typedef Record<key_t, value_t> record_t;

  F2RmwConditionalInsertContext(rmw_context_t* rmw_context_, bool rmw_rcu)
    : rmw_context{ rmw_context_ }
    , rmw_rcu_{ rmw_rcu }
  {}
  /// Copy constructor deleted; copy to tail request doesn't go async
  F2RmwConditionalInsertContext(const F2RmwConditionalInsertContext& from)
    : rmw_context{ from.rmw_context }
    , rmw_rcu_{ from.rmw_rcu_ }
  {}

 protected:
  /// The explicit interface requires a DeepCopy_Internal() implementation.
  Status DeepCopy_Internal(IAsyncContext*& context_copy) final {
    // Deep copy RmwRead context
    // NOTE: this may also trigger a deep copy for F2 RmwContext (if necessary)
    IAsyncContext* read_context_copy;
    rmw_read_context_t* read_context = static_cast<rmw_read_context_t*>(rmw_context->read_context);
    RETURN_NOT_OK(read_context->DeepCopy(read_context_copy));
    rmw_context = static_cast<rmw_context_t*>(read_context->f2_rmw_context);

    // Deep copy this context
    RETURN_NOT_OK(IAsyncContext::DeepCopy_Internal(*this, context_copy));
    rmw_context->ci_context = context_copy;

    return Status::Ok;
  }

 public:
  inline uint32_t key_size() const {
    return rmw_context->key_size();
  }
  inline KeyHash get_key_hash() const {
    return rmw_context->get_key_hash();
  }
  inline bool is_key_equal(const key_t& other) const {
    return rmw_context->is_key_equal(other);
  }
  inline void write_deep_key_at(key_t* dst) const {
    rmw_context->write_deep_key_at(dst);
  }

  inline uint32_t value_size() const {
    return rmw_context->value_size();
  }
  inline bool is_tombstone() const {
    return false; // rmw never copies tombstone records
  }
  inline Address orig_hlog_tail_address() const {
    return rmw_context->orig_hlog_tail_address;
  }

  inline bool Insert(void* dest, uint32_t alloc_size) const {
    record_t* record = reinterpret_cast<record_t*>(dest);
    // write key
    key_t* key_dest = const_cast<key_t*>(&record->key());
    rmw_context->write_deep_key_at(key_dest);
    // write value
    if (rmw_rcu_) {
      // insert updated value
      rmw_read_context_t* read_context = static_cast<rmw_read_context_t*>(rmw_context->read_context);
      rmw_context->RmwCopy(*read_context->value(), record->value());
    } else {
      // Insert initial value
      rmw_context->RmwInitial(record->value());
    }
    return true;
  }

  inline void Put(void* rec) {
    assert(false); // this should never be called
    throw std::runtime_error{ "Impossible call to Put method" };
  }
  inline bool PutAtomic(void* rec) {
    assert(false); // this should never be called
    throw std::runtime_error{ "Impossible call to PutAtomic method" };
    return false;
  }

 public:
  rmw_context_t* rmw_context;

 private:
  bool rmw_rcu_;
};

}
} // namespace FASTER::core
