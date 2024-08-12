/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef SRC_SQL_ENGINE_JOIN_HASH_JOIN_HASH_TABLE_H_
#define SRC_SQL_ENGINE_JOIN_HASH_JOIN_HASH_TABLE_H_

#include "sql/engine/join/hash_join/ob_hash_join_struct.h"
#include "lib/atomic/ob_atomic.h"

namespace oceanbase
{
namespace sql
{

struct Bucket
{
public:
  Bucket() : val_(0) {}
  explicit inline Bucket(uint64_t value) : val_(value) {}
  explicit inline Bucket(uint64_t salt, ObHJStoredRow *row_ptr) 
                    : row_ptr_(reinterpret_cast<uint64_t>(row_ptr) & POINTER_MASK),
                      salt_(salt) {}
  
  inline bool used() const { return 0 != val_; }
  inline uint64_t get_salt() { return salt_; }
  inline ObHJStoredRow *get_ptr() {
    return reinterpret_cast<ObHJStoredRow *>(row_ptr_);
  }
  inline void set_salt(uint64_t salt) { salt_ = salt; }
  inline void set_row_ptr(ObHJStoredRow *row_ptr) {
    row_ptr_ = reinterpret_cast<uint64_t>(row_ptr) & POINTER_MASK;
  }
  static inline uint64_t extract_salt(const uint64_t hash_val) {
    return (hash_val & SALT_MASK) >> POINTER_BIT;
  }
  static inline uint64_t extract_row_ptr(const uint64_t hash_val) {
    return (hash_val & POINTER_MASK);
  }

public:
  // Upper 16 bits are salt
	static constexpr const uint64_t SALT_MASK = 0xFFFF000000000000;
	// Lower 48 bits are the pointer
	static constexpr const uint64_t POINTER_MASK = 0x0000FFFFFFFFFFFF;
  static constexpr const uint64_t POINTER_BIT = 48;
  static constexpr const uint64_t END_ROW_PTR = 0x0000FFFFFFFFFFFF;
  union {
    struct {
      uint64_t row_ptr_:48;
      uint64_t salt_:16;
    };
    uint64_t val_;
  };
};

struct HashTable
{
public:
  HashTable()
    : buckets_(nullptr),
      nbuckets_(0),
      bucket_mask_(0),
      row_count_(0),
      collisions_(0),
      used_buckets_(0),
      inited_(false),
      ht_alloc_(nullptr)
  {
  }
  int init(ObIAllocator &alloc);
  int build_prepare(JoinTableCtx &ctx, int64_t row_count, int64_t bucket_count);
  virtual int insert_batch(JoinTableCtx &ctx,
                           ObHJStoredRow **stored_rows,
                           const int64_t size,
                           int64_t &used_buckets,
                           int64_t &collisions);
  int probe_prepare(JoinTableCtx &ctx, OutputInfo &output_info) { return OB_SUCCESS; }
  int probe_batch(JoinTableCtx &ctx, OutputInfo &output_info) {
    int ret = OB_SUCCESS;
    if (ctx.probe_opt_) {
      ret = probe_batch_opt(ctx, output_info);
    } else {
      bool has_other_conds = (0 != ctx.other_conds_->count());
      if (ctx.need_probe_del_match()) {
        ret = has_other_conds
                ? probe_batch_del_match<true>(ctx, output_info)
                : probe_batch_del_match<false>(ctx, output_info);
      } else {
        ret = has_other_conds
                ? probe_batch_normal<true>(ctx, output_info)
                : probe_batch_normal<false>(ctx, output_info);
      }
    }
    return ret;
  }
  int get_unmatched_rows(JoinTableCtx &ctx, OutputInfo &output_info);
  int project_matched_rows(JoinTableCtx &ctx, OutputInfo &output_info);
  void reset();
  void free(ObIAllocator *alloc);

  int64_t get_row_count() const { return row_count_; };
  int64_t get_used_buckets() const { return used_buckets_; }
  int64_t get_nbuckets() const { return nbuckets_; }
  int64_t get_collisions() const { return collisions_; }
  int64_t get_mem_used() const {
    int64_t size = 0;
    size = sizeof(*this);
    if (nullptr != buckets_) {
      size += (sizeof(Bucket) * nbuckets_);
    }
    return size;
  }
  int64_t get_one_bucket_size() const { return sizeof(Bucket); };
  int64_t get_normalized_key_size() const { return 0; }
  virtual void set_diag_info(int64_t used_buckets, int64_t collisions) {
    used_buckets_ += used_buckets;
    collisions_ += collisions;
  }

protected:
  static inline void key_equal(JoinTableCtx &ctx,
                               ObHJStoredRow *left_row, 
                               ObHJStoredRow *right_row,
                               bool &equal);
  static inline void key_equal(JoinTableCtx &ctx,
                               ObHJStoredRow *build_row, 
                               uint64_t probe_batch_idx,
                               bool &equal);
  static inline int calc_other_join_conditions_batch(JoinTableCtx &ctx);
  int probe_batch_opt(JoinTableCtx &ctx, OutputInfo &output_info);
  
  template <bool HAS_OTHER_CONDS = false>
  int probe_batch_normal(JoinTableCtx &ctx, OutputInfo &output_info);

  template <bool HAS_OTHER_CONDS = false>
  int probe_batch_del_match(JoinTableCtx &ctx, OutputInfo &output_info);

protected:
  Bucket *buckets_;
  int64_t nbuckets_;
  int64_t bucket_mask_;
  int64_t row_count_;
  int64_t collisions_;
  int64_t used_buckets_;
  bool inited_;
  ModulePageAllocator *ht_alloc_;
};

struct SharedHashTable final : public HashTable
{
public:
  virtual int insert_batch(JoinTableCtx &ctx,
                           ObHJStoredRow **stored_rows,
                           const int64_t size,
                           int64_t &used_buckets,
                           int64_t &collisions) override;
  inline virtual void set_diag_info(int64_t used_buckets, int64_t collisions) override {
    ATOMIC_AAF(&used_buckets_, used_buckets);
    ATOMIC_AAF(&collisions_, collisions);
  }
};

struct RobinHashTable final : public HashTable
{
public:
  int insert_batch(JoinTableCtx &ctx,
                   ObHJStoredRow **stored_rows,
                   const int64_t size,
                   int64_t &used_buckets,
                   int64_t &collisions) override;
private:
  // Get the distance between cur_bkt_pos and opt_bkt_pos.
  inline uint64_t dist_from_opt(uint64_t hash_val, uint64_t cur_bkt_pos);
  // Put bucket in buckets_[pos], and move the subsequent buckets back.
  inline void set_and_shift_up(Bucket bucket, uint64_t pos);
};


} // end namespace sql
} // end namespace oceanbase

#include "sql/engine/join/hash_join/hash_table.ipp"

#endif /* SRC_SQL_ENGINE_JOIN_HASH_JOIN_HASH_TABLE_H_*/