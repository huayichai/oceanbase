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
#define USING_LOG_PREFIX SQL_ENG

#include "sql/engine/join/hash_join/hash_table.h"
#include "share/vector/ob_fixed_length_vector.h"
#include "share/vector/ob_continuous_vector.h"
#include "share/vector/ob_uniform_vector.h"
#include "share/vector/ob_discrete_vector.h"
#include "lib/atomic/ob_atomic.h"

namespace oceanbase
{
namespace sql
{

int HashTable::init(ObIAllocator &alloc) {
  int ret = OB_SUCCESS;
  if (!inited_) {
    void *alloc_buf = alloc.alloc(sizeof(ModulePageAllocator));
    if (OB_ISNULL(alloc_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory", K(ret));
    } else {
      ht_alloc_ = new (alloc_buf) ModulePageAllocator(alloc);
      ht_alloc_->set_label("HtOpAlloc");
      inited_ = true;
    }
  }
  return ret;
}

int HashTable::build_prepare(JoinTableCtx &ctx, int64_t row_count, int64_t bucket_count)
{
  int ret = OB_SUCCESS;
  row_count_ = row_count;
  nbuckets_ = std::max(nbuckets_, bucket_count);
  bucket_mask_ = nbuckets_ - 1;
  collisions_ = 0;
  used_buckets_ = 0;
  ctx.cur_tuple_ = reinterpret_cast<void *>(Bucket::END_ROW_PTR);
  
  if (nullptr == buckets_) {
    ht_alloc_->free(buckets_);
    buckets_ = nullptr;
  }
  int64_t buckets_size = sizeof(Bucket) * nbuckets_;
  buckets_ = reinterpret_cast<Bucket *>(ht_alloc_->alloc(buckets_size));
  MEMSET(buckets_, 0, buckets_size);
  if (OB_ISNULL(buckets_)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory", K(ret));
  }

  // if (OB_SUCC(ret) && !ctx.probe_opt_) {
  //   void *sel_buf = ht_alloc_->alloc(
  //                     sizeof(*ctx.unmatch_sel_)
  //                     * ctx.max_batch_size_);
  //   void *rows_buf = ht_alloc_->alloc(
  //                     sizeof(*ctx.unmatch_rows_)
  //                     * ctx.max_batch_size_);
  //   void *match_buf = ht_alloc_->alloc(
  //                     sizeof(*ctx.join_cond_match_)
  //                     * ctx.max_batch_size_);
  //   void *skip_buf = ht_alloc_->alloc(
  //                     ObBitVector::memory_size(ctx.max_batch_size_));
  //   if (OB_ISNULL(sel_buf) || OB_ISNULL(rows_buf) 
  //       || OB_ISNULL(match_buf) || OB_ISNULL(skip_buf)) {
  //     ret = OB_ALLOCATE_MEMORY_FAILED;
  //     if (OB_NOT_NULL(sel_buf)) {
  //       ht_alloc_->free(sel_buf);
  //       sel_buf = nullptr;
  //     }
  //     if (OB_NOT_NULL(rows_buf)) {
  //       ht_alloc_->free(rows_buf);
  //       rows_buf = nullptr;
  //     }
  //     if (OB_NOT_NULL(match_buf)) {
  //       ht_alloc_->free(match_buf);
  //       match_buf = nullptr;
  //     }
  //     if (OB_NOT_NULL(skip_buf)) {
  //       ht_alloc_->free(skip_buf);
  //       skip_buf = nullptr;
  //     }
  //     LOG_WARN("failed to alloc memory", K(ret));
  //   } else {
  //     ctx.unmatch_sel_ = reinterpret_cast<uint16_t *>(sel_buf);
  //     ctx.unmatch_rows_ = reinterpret_cast<ObHJStoredRow **>(rows_buf);
  //     ctx.join_cond_match_ = reinterpret_cast<bool *>(match_buf);
  //     ctx.eval_skip_ = reinterpret_cast<ObBitVector *>(skip_buf);
  //   }
  //   if (OB_SUCC(ret) && ctx.need_probe_del_match()) {
  //     void *bkt_buf = ht_alloc_->alloc(
  //                       sizeof(*ctx.cur_bkts_)
  //                       * ctx.max_batch_size_);
  //     void *prev_rows_buf = ht_alloc_->alloc(
  //                             sizeof(*ctx.prev_rows_)
  //                             * ctx.max_batch_size_);
  //     if (OB_ISNULL(bkt_buf) || OB_ISNULL(prev_rows_buf)) {
  //       ret = OB_ALLOCATE_MEMORY_FAILED;
  //       if (OB_NOT_NULL(bkt_buf)) {
  //         ht_alloc_->free(bkt_buf);
  //         bkt_buf = nullptr;
  //       }
  //       if (OB_NOT_NULL(prev_rows_buf)) {
  //         ht_alloc_->free(prev_rows_buf);
  //         prev_rows_buf = nullptr;
  //       }
  //       LOG_WARN("failed to alloc memory", K(ret));
  //     } else {
  //       ctx.cur_bkts_ = reinterpret_cast<Bucket **>(bkt_buf);
  //       ctx.prev_rows_ = reinterpret_cast<ObHJStoredRow **>(prev_rows_buf);
  //     }
  //   }
  // }

  LOG_DEBUG("build prepare", K(row_count), K(bucket_count), K(nbuckets_), K(sizeof(Bucket)));
  return ret;
}

int HashTable::insert_batch(JoinTableCtx &ctx,
                            ObHJStoredRow **stored_rows,
                            const int64_t size,
                            int64_t &used_buckets,
                            int64_t &collisions)
{
  int ret = OB_SUCCESS;
  const RowMeta &row_meta = ctx.build_row_meta_;

  // prefetch bucket
  for (int64_t i = 0; i < size; i++) {
    uint64_t pos = stored_rows[i]->get_hash_value(ctx.build_row_meta_) & bucket_mask_;
    __builtin_prefetch(&buckets_[pos], 1 /* write */, 3 /* high temporal locality*/);
  }

  // set row_ptr
  for (int64_t row_idx = 0; row_idx < size; row_idx++) {
    ObHJStoredRow *row_ptr = stored_rows[row_idx];
    uint64_t hash_val = row_ptr->get_hash_value(row_meta);
    uint64_t salt = Bucket::extract_salt(hash_val);
    uint64_t pos = hash_val & bucket_mask_;
    Bucket *bucket;
    bool equal;
    while (true) {
      bucket = &buckets_[pos];
      if (!bucket->used()) {
        bucket->set_salt(salt);
        bucket->set_row_ptr(row_ptr);
        row_ptr->set_next(row_meta, 
                          reinterpret_cast<ObHJStoredRow *>(Bucket::END_ROW_PTR));
        used_buckets++;
        break;
      } else if (salt == bucket->get_salt()) {
        ObHJStoredRow *left_ptr = reinterpret_cast<ObHJStoredRow *>(bucket->get_ptr());
        key_equal(ctx, left_ptr, row_ptr, equal);
        if (equal) {
          row_ptr->set_next(row_meta, left_ptr);
          bucket->set_row_ptr(row_ptr);
          break;
        }
      }
      pos = (pos + 1) & bucket_mask_;
      collisions++;
    }
  }
  
  return ret;
}

void HashTable::key_equal(JoinTableCtx &ctx,
                         ObHJStoredRow *left_row,
                         ObHJStoredRow *right_row,
                         bool &equal)
{
  equal = true;
  for (int64_t i = 0; i < ctx.build_key_proj_->count(); i++) {
    int64_t build_col_idx = ctx.build_key_proj_->at(i);
    const char *v1 = NULL, *v2 = NULL;
    ObLength len1 = 0, len2 = 0;
    left_row->get_cell_payload(ctx.build_row_meta_, build_col_idx, v1, len1);
    right_row->get_cell_payload(ctx.build_row_meta_, build_col_idx, v2, len2);
    
    if ((len1 != len2) || (0 != MEMCMP(v1, v2, len1))) {
      equal = false;
      break;
    }
  }
  // if (left_row->get_hash_value(ctx.build_row_meta_) 
  //     != right_row->get_hash_value(ctx.build_row_meta_)) {
  //   equal = false;
  // } else {
  //   for (int64_t i = 0; i < ctx.build_key_proj_->count(); i++) {
  //     int64_t build_col_idx = ctx.build_key_proj_->at(i);
  //     const char *v1 = NULL, *v2 = NULL;
  //     ObLength len1 = 0, len2 = 0;
  //     left_row->get_cell_payload(ctx.build_row_meta_, build_col_idx, v1, len1);
  //     right_row->get_cell_payload(ctx.build_row_meta_, build_col_idx, v2, len2);
      
  //     if ((len1 != len2) || (0 != MEMCPY(v1, v2, len1))) {
  //       equal = false;
  //       break;
  //     }
  //   }
  // }
}

void HashTable::key_equal(JoinTableCtx &ctx,
                          ObHJStoredRow *build_row, 
                          uint64_t probe_batch_idx,
                          bool &equal)
{
  equal = true;
  int cmp_ret = 0;
  for (int64_t i = 0; i < ctx.build_key_proj_->count(); i++) {
    int64_t build_col_idx = ctx.build_key_proj_->at(i);
    ObExpr *probe_key_expr = ctx.probe_keys_->at(i);
    ObIVector *probe_key_vec = probe_key_expr->get_vector(*ctx.eval_ctx_);
    const char *r_v = NULL;
    ObLength r_len = 0;
    build_row->get_cell_payload(ctx.build_row_meta_, build_col_idx, r_v, r_len);
    probe_key_vec->null_first_cmp(*probe_key_expr, probe_batch_idx,
                                  build_row->is_null(build_col_idx),
                                  r_v, r_len, cmp_ret);
    equal = (cmp_ret == 0);
    if (!equal) {
      break;
    }
  }
}

int HashTable::calc_other_join_conditions_batch(JoinTableCtx &ctx)
{
  int ret = OB_SUCCESS;
  int64_t batch_idx;
  bool all_rows_active = (ctx.unmatch_sel_cnt_
                          == ctx.probe_batch_rows_->brs_.size_);
  if (all_rows_active) {
    ctx.eval_skip_->reset(ctx.probe_batch_rows_->brs_.size_);
    for (uint16_t i = 0; i < ctx.unmatch_sel_cnt_; ++i) {
        ctx.clear_one_row_eval_flag(ctx.unmatch_sel_[i]);
        ctx.join_cond_match_[i] = true;
    }
  } else {
    ctx.eval_skip_->set_all(ctx.probe_batch_rows_->brs_.size_);
    for (uint16_t i = 0; i < ctx.unmatch_sel_cnt_; ++i) {
        batch_idx = ctx.unmatch_sel_[i];
        ctx.clear_one_row_eval_flag(batch_idx);
        ctx.eval_skip_->unset(batch_idx);
        ctx.join_cond_match_[i] = true;
    }
  }
  
  if (OB_FAIL(ObHJStoredRow::convert_rows_to_exprs(
                              *ctx.build_output_,
                              *ctx.eval_ctx_,
                              ctx.build_row_meta_,
                              const_cast<const ObHJStoredRow**>(
                                ctx.unmatch_rows_),
                              ctx.unmatch_sel_,
                              ctx.unmatch_sel_cnt_))) {
    LOG_WARN("failed to convert expr", K(ret));
  } else {
    const ExprFixedArray *conds = ctx.other_conds_;
    uint16_t remaining_cnt = ctx.unmatch_sel_cnt_;
    ARRAY_FOREACH(*conds, i) {
      if (0 == remaining_cnt) {
        break;
      }
      ObExpr *expr = conds->at(i);
      if (OB_FAIL(expr->eval_vector(*ctx.eval_ctx_,
                                    *ctx.eval_skip_,
                                    ctx.probe_batch_rows_->brs_.size_,
                                    all_rows_active))) {
        LOG_WARN("fail to eval vector", K(ret));
      } else {
        if (is_uniform_format(expr->get_format(*ctx.eval_ctx_))) {
          ObUniformBase *uni_vec = static_cast<ObUniformBase *>(
                                                  expr->get_vector(*ctx.eval_ctx_));
          for (int64_t j = 0; j < ctx.unmatch_sel_cnt_; ++j) {
            batch_idx = ctx.unmatch_sel_[j];
            if (ctx.eval_skip_->at(batch_idx)) {
              continue;
            }
            if ((uni_vec->is_null(batch_idx) || 0 == uni_vec->get_int(batch_idx))) {
              ctx.join_cond_match_[j] = false;
              ctx.eval_skip_->set(batch_idx);
              all_rows_active = false;
              remaining_cnt--;
            }
          }
        } else {
          ObFixedLengthBase *fixed_vec = static_cast<ObFixedLengthBase *>(
                                                  expr->get_vector(*ctx.eval_ctx_));
          for (int64_t j = 0; j < ctx.unmatch_sel_cnt_; ++j) {
            batch_idx = ctx.unmatch_sel_[j];
            if (ctx.eval_skip_->at(batch_idx)) {
              continue;
            }
            if ((fixed_vec->is_null(batch_idx) || 0 == fixed_vec->get_int(batch_idx))) {
              ctx.join_cond_match_[j] = false;
              ctx.eval_skip_->set(batch_idx);
              all_rows_active = false;
              remaining_cnt--;
            }                                
          }
        }
      }
    } // for end
  }
  return ret;
}

int HashTable::probe_batch_opt(JoinTableCtx &ctx, OutputInfo &output_info)
{
  int ret = OB_SUCCESS;
  if (output_info.first_probe_) {
    uint64_t *hash_vals = ctx.probe_batch_rows_->hash_vals_;
    for (int64_t i = 0; i < output_info.selector_cnt_; i++) {
      uint64_t hash_val = hash_vals[output_info.selector_[i]];
      ObHJStoredRow *row_ptr = buckets_[hash_val & bucket_mask_].get_ptr();
      if (Bucket::END_ROW_PTR != reinterpret_cast<uint64_t>(row_ptr)) {
        __builtin_prefetch(row_ptr, 0, 1 /*low temporal locality*/);
      }
    }
    uint64_t key_match_idx = 0;
    for (int64_t i = 0; i < output_info.selector_cnt_; i++) {
      uint64_t batch_idx = output_info.selector_[i];
      uint64_t hash_val = hash_vals[batch_idx];
      uint64_t salt = Bucket::extract_salt(hash_val);
      uint64_t pos = hash_val & bucket_mask_;
      Bucket *bucket;
      bool equal;
      while (true) {
        bucket = &buckets_[pos];
        if (!bucket->used()) {
          bucket = nullptr;
          break;
        } else if (salt == bucket->get_salt()) {
          ObHJStoredRow *build_row_ptr = reinterpret_cast<ObHJStoredRow *>(bucket->get_ptr());
          key_equal(ctx, build_row_ptr, batch_idx, equal);
          if (equal) {
            output_info.left_result_rows_[key_match_idx] = build_row_ptr;
            ctx.scan_chain_rows_[key_match_idx] = build_row_ptr->get_next(ctx.build_row_meta_);
            output_info.selector_[key_match_idx++] = batch_idx;
            if (ctx.need_mark_match()) {
              build_row_ptr->set_is_match(ctx.build_row_meta_, true);
            }
            break;
          }
        }
        pos = (pos + 1) & bucket_mask_;
      }
    }
    output_info.selector_cnt_ = key_match_idx;
    output_info.first_probe_ = false;
  } else {
    uint64_t key_match_idx = 0;
    for (int64_t i = 0; i < output_info.selector_cnt_; i++) {
      uint64_t batch_idx = output_info.selector_[i];
      ObHJStoredRow *build_row_ptr = ctx.scan_chain_rows_[i];
      if (Bucket::END_ROW_PTR == reinterpret_cast<uint64_t>(build_row_ptr)) {
        continue;
      }
      output_info.left_result_rows_[key_match_idx] = build_row_ptr;
      ctx.scan_chain_rows_[key_match_idx] = build_row_ptr->get_next(ctx.build_row_meta_);
      output_info.selector_[key_match_idx++] = batch_idx;
      if (ctx.need_mark_match()) {
        build_row_ptr->set_is_match(ctx.build_row_meta_, true);
      }
    }
    output_info.selector_cnt_ = key_match_idx;
  }
  return ret;
}

template <bool HAS_OTHER_CONDS>
int HashTable::probe_batch_normal(JoinTableCtx &ctx, OutputInfo &output_info)
{
  int ret = OB_SUCCESS;
  uint64_t key_match_idx = 0;
  uint64_t key_unmatch_idx = 0;

  // get equal-key rows
  if (output_info.first_probe_) {
    uint64_t *hash_vals = ctx.probe_batch_rows_->hash_vals_;
    
    // prefetch
    for (int64_t i = 0; i < output_info.selector_cnt_; i++) {
      uint64_t hash_val = hash_vals[output_info.selector_[i]];
      ObHJStoredRow *row_ptr = buckets_[hash_val & bucket_mask_].get_ptr();
      if (Bucket::END_ROW_PTR != reinterpret_cast<uint64_t>(row_ptr)) {
        __builtin_prefetch(row_ptr, 0, HAS_OTHER_CONDS 
                                        ? 2 /*high temporal locality*/ 
                                        : 1 /*low temporal locality*/);
      }
    }
    
    // salt and key compare
    key_match_idx = 0;
    for (int64_t i = 0; i < output_info.selector_cnt_; i++) {
      uint64_t batch_idx = output_info.selector_[i];
      uint64_t hash_val = hash_vals[batch_idx];
      uint64_t salt = Bucket::extract_salt(hash_val);
      uint64_t pos = hash_val & bucket_mask_;
      Bucket *bucket;
      bool equal;
      while (true) {
        bucket = &buckets_[pos];
        if (!bucket->used()) {
          bucket = nullptr;
          break;
        } else if (salt == bucket->get_salt()) {
          ObHJStoredRow *build_row_ptr = reinterpret_cast<ObHJStoredRow *>(bucket->get_ptr());
          key_equal(ctx, build_row_ptr, batch_idx, equal);
          if (equal) {
            if (HAS_OTHER_CONDS) {
              ctx.unmatch_rows_[key_match_idx] = build_row_ptr;
              ctx.unmatch_sel_[key_match_idx++] = batch_idx;
            } else {
              output_info.left_result_rows_[key_match_idx] = build_row_ptr;
              ctx.scan_chain_rows_[key_match_idx] = build_row_ptr->get_next(ctx.build_row_meta_);
              output_info.selector_[key_match_idx++] = batch_idx;
              if (ctx.need_mark_match()) {
                build_row_ptr->set_is_match(ctx.build_row_meta_, true);
              }
            }
            break;
          }
        }
        pos = (pos + 1) & bucket_mask_;
      }
    }
    output_info.first_probe_ = false;
  } else {
    key_match_idx = 0;
    for (int64_t i = 0; i < output_info.selector_cnt_; i++) {
      uint64_t batch_idx = output_info.selector_[i];
      ObHJStoredRow *build_row_ptr = ctx.scan_chain_rows_[i];
      if (Bucket::END_ROW_PTR == reinterpret_cast<uint64_t>(build_row_ptr)) {
        continue;
      }
      if (HAS_OTHER_CONDS) {
        ctx.unmatch_rows_[key_match_idx] = build_row_ptr;
        ctx.unmatch_sel_[key_match_idx++] = batch_idx;
      } else {
        output_info.left_result_rows_[key_match_idx] = build_row_ptr;
        ctx.scan_chain_rows_[key_match_idx] = build_row_ptr->get_next(ctx.build_row_meta_);
        output_info.selector_[key_match_idx++] = batch_idx;
        if (ctx.need_mark_match()) {
          build_row_ptr->set_is_match(ctx.build_row_meta_, true);
        }
      }
    }
  }
  ctx.unmatch_sel_cnt_ = key_match_idx;
  output_info.selector_cnt_ = key_match_idx;

  // calc other join conditions
  if (HAS_OTHER_CONDS) {
    key_match_idx = 0;
    while (OB_SUCC(ret) && 0 < ctx.unmatch_sel_cnt_) {
      if (OB_FAIL(calc_other_join_conditions_batch(ctx))) {
        LOG_WARN("fail to calc conditions", K(ret));
      } else {
        key_unmatch_idx = 0;
        for (uint16_t i = 0; i < ctx.unmatch_sel_cnt_; i++) {
          uint16_t batch_idx = ctx.unmatch_sel_[i];
          ObHJStoredRow *row_ptr = reinterpret_cast<ObHJStoredRow *>(
                                          ctx.unmatch_rows_[i]);
          ObHJStoredRow *next_row_ptr = row_ptr->get_next(ctx.build_row_meta_);
          if (!ctx.join_cond_match_[i]) {
            if (Bucket::END_ROW_PTR != reinterpret_cast<uint64_t>(next_row_ptr)) {
              ctx.unmatch_rows_[key_unmatch_idx] = next_row_ptr;
              ctx.unmatch_sel_[key_unmatch_idx++] = batch_idx;
            }
          } else {
            output_info.left_result_rows_[key_match_idx] = row_ptr;
            ctx.scan_chain_rows_[key_match_idx] = next_row_ptr;
            output_info.selector_[key_match_idx++] = batch_idx;
            if (ctx.need_mark_match()) {
              row_ptr->set_is_match(ctx.build_row_meta_, true);
            }
          }
        }
        ctx.unmatch_sel_cnt_ = key_unmatch_idx;
      }
    }
    output_info.selector_cnt_ = key_match_idx;
  } else if (OB_FAIL(ObHJStoredRow::convert_rows_to_exprs(
                              *ctx.build_output_,
                              *ctx.eval_ctx_,
                              ctx.build_row_meta_,
                              output_info.left_result_rows_,
                              output_info.selector_,
                              output_info.selector_cnt_))) {
    LOG_WARN("failed to convert expr", K(ret));
  }

  return ret;
}

template <bool HAS_OTHER_CONDS>
int HashTable::probe_batch_del_match(JoinTableCtx &ctx, OutputInfo &output_info)
{
  int ret = OB_SUCCESS;
  uint64_t key_match_idx = 0;
  uint64_t key_unmatch_idx = 0;
  uint64_t *hash_vals = ctx.probe_batch_rows_->hash_vals_;

  if (output_info.first_probe_) {
    // prefetch
    for (int64_t i = 0; i < output_info.selector_cnt_; i++) {
      uint64_t hash_val = hash_vals[output_info.selector_[i]];
      ObHJStoredRow *row_ptr = buckets_[hash_val & bucket_mask_].get_ptr();
      if (Bucket::END_ROW_PTR != reinterpret_cast<uint64_t>(row_ptr)) {
        __builtin_prefetch(row_ptr, 0, HAS_OTHER_CONDS 
                                        ? 2 /*high temporal locality*/ 
                                        : 1 /*low temporal locality*/);
      }
    }

    // get equal-key rows
    for (int64_t i = 0; i < output_info.selector_cnt_; i++) {
      uint64_t batch_idx = output_info.selector_[i];
      uint64_t hash_val = hash_vals[batch_idx];
      uint64_t salt = Bucket::extract_salt(hash_val);
      uint64_t pos = hash_val & bucket_mask_;
      Bucket *bucket;
      bool equal;
      while (true) {
        bucket = &buckets_[pos];
        if (!bucket->used()) {
          bucket = nullptr;
          break;
        } else if (salt == bucket->get_salt()) {
          ObHJStoredRow *build_row_ptr = reinterpret_cast<ObHJStoredRow *>(bucket->get_ptr());
          if (Bucket::END_ROW_PTR != reinterpret_cast<uint64_t>(build_row_ptr)) {
            key_equal(ctx, build_row_ptr, batch_idx, equal);
            if (equal) {
              if (HAS_OTHER_CONDS) {
                ctx.unmatch_bkts_[key_match_idx] = bucket;
                ctx.unmatch_prev_rows_[key_match_idx] = reinterpret_cast<ObHJStoredRow *>(Bucket::END_ROW_PTR);
                ctx.unmatch_rows_[key_match_idx] = build_row_ptr;
                ctx.unmatch_sel_[key_match_idx++] = batch_idx;
              } else {
                ctx.cur_bkts_[key_match_idx] = bucket;
                ctx.scan_chain_rows_[key_match_idx] = build_row_ptr->get_next(ctx.build_row_meta_);
                output_info.left_result_rows_[key_match_idx] = build_row_ptr;
                output_info.selector_[key_match_idx++] = batch_idx;
                // delete from list
                bucket->set_row_ptr(build_row_ptr->get_next(ctx.build_row_meta_));
                build_row_ptr->set_is_match(ctx.build_row_meta_, true);
                row_count_--;
              }
              break;
            }
          }
        }
        pos = (pos + 1) & bucket_mask_;
      }
    }
    output_info.first_probe_ = false;
  } else {
    key_match_idx = 0;
    for (int64_t i = 0; i < output_info.selector_cnt_; i++) {
      uint64_t batch_idx = output_info.selector_[i];
      ObHJStoredRow *build_row_ptr = ctx.scan_chain_rows_[i];
      Bucket *bucket = reinterpret_cast<Bucket *>(ctx.cur_bkts_[i]);
      if (Bucket::END_ROW_PTR == reinterpret_cast<uint64_t>(build_row_ptr)) {
        continue;
      }
      if (HAS_OTHER_CONDS) {
        ctx.unmatch_bkts_[key_match_idx] = bucket;
        ctx.unmatch_prev_rows_[key_match_idx] = ctx.prev_rows_[i];
        ctx.unmatch_rows_[key_match_idx] = build_row_ptr;
        ctx.unmatch_sel_[key_match_idx++] = batch_idx;
      } else {
        if (build_row_ptr->is_match(ctx.build_row_meta_)) {
          continue;
        }
        ctx.cur_bkts_[key_match_idx] = bucket;
        ctx.scan_chain_rows_[key_match_idx] = build_row_ptr->get_next(ctx.build_row_meta_);
        output_info.left_result_rows_[key_match_idx] = build_row_ptr;
        output_info.selector_[key_match_idx++] = batch_idx;
        bucket->set_row_ptr(build_row_ptr->get_next(ctx.build_row_meta_));
        row_count_--;
      }
    }
  }
  
  ctx.unmatch_sel_cnt_ = key_match_idx;
  output_info.selector_cnt_ = key_match_idx;

  // calc other join conditions
  if (HAS_OTHER_CONDS) {
    key_match_idx = 0;
    while (OB_SUCC(ret) && 0 < ctx.unmatch_sel_cnt_) {
      if (OB_FAIL(calc_other_join_conditions_batch(ctx))) {
        LOG_WARN("fail to calc conditions", K(ret));
      } else {
        key_unmatch_idx = 0;
        for (uint16_t i = 0; i < ctx.unmatch_sel_cnt_; i++) {
          uint16_t batch_idx = ctx.unmatch_sel_[i];
          ObHJStoredRow *row_ptr = ctx.unmatch_rows_[i];
          ObHJStoredRow *next_row_ptr = row_ptr->get_next(ctx.build_row_meta_);
          if (!ctx.join_cond_match_[i]) {
            if (Bucket::END_ROW_PTR != reinterpret_cast<uint64_t>(next_row_ptr)) {
              ctx.unmatch_bkts_[key_unmatch_idx] = ctx.unmatch_bkts_[i];
              ctx.unmatch_prev_rows_[key_unmatch_idx] = row_ptr;
              ctx.unmatch_rows_[key_unmatch_idx] = next_row_ptr;
              ctx.unmatch_sel_[key_unmatch_idx++] = batch_idx;
            }
          } else if (!row_ptr->is_match(ctx.build_row_meta_)) {
            ctx.cur_bkts_[key_match_idx] = ctx.unmatch_bkts_[i];
            ctx.prev_rows_[key_match_idx] = ctx.unmatch_prev_rows_[i];
            ctx.scan_chain_rows_[key_match_idx] = next_row_ptr;
            output_info.left_result_rows_[key_match_idx] = row_ptr;
            output_info.selector_[key_match_idx++] = batch_idx;
            //avoid duplicate match in one batch
            row_ptr->set_is_match(ctx.build_row_meta_, true);
            ObHJStoredRow *prev_row = reinterpret_cast<ObHJStoredRow *>(ctx.unmatch_prev_rows_[i]);
            if (Bucket::END_ROW_PTR == reinterpret_cast<uint64_t>(prev_row)) {
              reinterpret_cast<Bucket *>(ctx.unmatch_bkts_[i])->set_row_ptr(next_row_ptr);
            } else {
              prev_row->set_next(ctx.build_row_meta_, next_row_ptr);
            }
            row_count_--;
          }
        }
        ctx.unmatch_sel_cnt_ = key_unmatch_idx;
      }
    }
    output_info.selector_cnt_ = key_match_idx;
  } else if (OB_FAIL(ObHJStoredRow::convert_rows_to_exprs(
                              *ctx.build_output_,
                              *ctx.eval_ctx_,
                              ctx.build_row_meta_,
                              output_info.left_result_rows_,
                              output_info.selector_,
                              output_info.selector_cnt_))) {
    LOG_WARN("failed to convert expr", K(ret));
  }

  return ret;
}

int HashTable::get_unmatched_rows(JoinTableCtx &ctx, OutputInfo &output_info)
{
  int ret = OB_SUCCESS;
  ObHJStoredRow *row_ptr = reinterpret_cast<ObHJStoredRow *>(ctx.cur_tuple_);
  int64_t batch_idx = 0;
  while (OB_SUCC(ret) && batch_idx < *ctx.max_output_cnt_) {
    if (Bucket::END_ROW_PTR != reinterpret_cast<uint64_t>(row_ptr)) {
      if (!row_ptr->is_match(ctx.build_row_meta_)) {
        output_info.left_result_rows_[batch_idx] = row_ptr;
        batch_idx++;
      }
      row_ptr = row_ptr->get_next(ctx.build_row_meta_);
    } else {
      int64_t bucket_id = ctx.cur_bkid_ + 1;
      if (bucket_id < nbuckets_) {
        Bucket &bkt = buckets_[bucket_id];
        row_ptr = bkt.used()
                    ? bkt.get_ptr() 
                    : reinterpret_cast<ObHJStoredRow *>(Bucket::END_ROW_PTR);
        ctx.cur_bkid_ = bucket_id;
      } else {
        ret = OB_ITER_END;
      }
    }
  }
  output_info.selector_cnt_ = batch_idx;
  ctx.cur_tuple_ = row_ptr;

  return ret;
}

int HashTable::project_matched_rows(JoinTableCtx &ctx, OutputInfo &output_info)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObHJStoredRow::attach_rows(*ctx.build_output_,
                                         *ctx.eval_ctx_,
                                         ctx.build_row_meta_,
                                         output_info.left_result_rows_,
                                         output_info.selector_,
                                         output_info.selector_cnt_))) {
    LOG_WARN("fail to attach rows",  K(ret));
  }
  return ret;
}

void HashTable::reset()
{
  if (nullptr != buckets_) {
    ht_alloc_->free(buckets_);
    buckets_ = nullptr;
  }
  nbuckets_ = 0;
  bucket_mask_ = 0;
  row_count_ = 0;
  collisions_ = 0;
  used_buckets_ = 0;
  inited_ = false;
}

void HashTable::free(ObIAllocator *alloc)
{
  reset();
  if (nullptr != ht_alloc_) {
    ht_alloc_->reset();
    ht_alloc_->~ModulePageAllocator();
    alloc->free(ht_alloc_);
    ht_alloc_ = nullptr;
  }
}

int SharedHashTable::insert_batch(JoinTableCtx &ctx,
                                  ObHJStoredRow **stored_rows,
                                  const int64_t size,
                                  int64_t &used_buckets,
                                  int64_t &collisions)
{
  int ret = OB_SUCCESS;
  const RowMeta &row_meta = ctx.build_row_meta_;

  // set row_ptr
  for (int64_t row_idx = 0; row_idx < size; row_idx++) {
    ObHJStoredRow *row_ptr = stored_rows[row_idx];
    uint64_t hash_val = row_ptr->get_hash_value(row_meta);
    uint64_t salt = Bucket::extract_salt(hash_val);
    uint64_t pos = hash_val & bucket_mask_;
    Bucket *bucket;
    Bucket old_bucket, new_bucket;
    new_bucket.set_salt(salt);
    new_bucket.set_row_ptr(row_ptr);
    bool equal = false;
    bool added = false;
    for (int64_t i = 0; i < nbuckets_; i += 1, pos = ((pos + 1) & bucket_mask_)) {
      bucket = &buckets_[pos];
      do {
        old_bucket.val_ = ATOMIC_LOAD(&bucket->val_);
        if (!old_bucket.used()) {
          row_ptr->set_next(row_meta, 
                            reinterpret_cast<ObHJStoredRow *>(Bucket::END_ROW_PTR));
          if (ATOMIC_BCAS(&bucket->val_, old_bucket.val_, new_bucket.val_)) {
            added = true;
            used_buckets++;
          }
        } else if (salt == old_bucket.get_salt()) {
          ObHJStoredRow *left_ptr = reinterpret_cast<ObHJStoredRow *>(old_bucket.get_ptr());
          key_equal(ctx, left_ptr, row_ptr, equal);
          if (!equal) {
            break;
          }
          row_ptr->set_next(row_meta, left_ptr);
          if (ATOMIC_BCAS(&bucket->val_, old_bucket.val_, new_bucket.val_)) {
            added = true;
          }
        } else {
          break;
        }
      } while (!added);
      if (added) {
        break;
      }
      collisions++;
    }
  }
  
  return ret;
}

uint64_t RobinHashTable::dist_from_opt(uint64_t hash_val, uint64_t cur_bkt_pos)
{
  uint64_t opt_bkt_pos = hash_val & this->bucket_mask_;
  return cur_bkt_pos >= opt_bkt_pos 
          ? cur_bkt_pos - opt_bkt_pos 
          : this->nbuckets_ + cur_bkt_pos - opt_bkt_pos;
}

void RobinHashTable::set_and_shift_up(Bucket bucket, uint64_t pos)
{
  do {
    Bucket &cur_bucket = this->buckets_[pos];
    if (!cur_bucket.used()) {
      cur_bucket = bucket;
      break;
    }
    std::swap(cur_bucket, bucket);
    pos = (pos + 1) & this->bucket_mask_;
  } while (true);
}

int RobinHashTable::insert_batch(JoinTableCtx &ctx,
                                 ObHJStoredRow **stored_rows,
                                 const int64_t size,
                                 int64_t &used_buckets,
                                 int64_t &collisions)
{
  int ret = OB_SUCCESS;
  const RowMeta &row_meta = ctx.build_row_meta_;

  // prefetch bucket
  for (int64_t i = 0; i < size; i++) {
    uint64_t pos = stored_rows[i]->get_hash_value(ctx.build_row_meta_) & bucket_mask_;
    __builtin_prefetch(&buckets_[pos], 1 /* write */, 3 /* high temporal locality*/);
  }

  // set row_ptr
  for (int64_t row_idx = 0; row_idx < size; row_idx++) {
    ObHJStoredRow *row_ptr = stored_rows[row_idx];
    uint64_t hash_val = row_ptr->get_hash_value(row_meta);
    uint64_t salt = Bucket::extract_salt(hash_val);
    uint64_t pos = hash_val & bucket_mask_;
    uint64_t dist = 0;
    Bucket *bucket;
    bool equal;
    while (true) {
      bucket = &buckets_[pos];
      if (!bucket->used()) {
        bucket->set_salt(salt);
        bucket->set_row_ptr(row_ptr);
        row_ptr->set_next(row_meta, 
                          reinterpret_cast<ObHJStoredRow *>(Bucket::END_ROW_PTR));
        used_buckets++;
        break;
      } else if (salt == bucket->get_salt()) {
        ObHJStoredRow *left_ptr = reinterpret_cast<ObHJStoredRow *>(bucket->get_ptr());
        key_equal(ctx, left_ptr, row_ptr, equal);
        if (equal) {
          row_ptr->set_next(row_meta, left_ptr);
          bucket->set_row_ptr(row_ptr);
          break;
        } else if (dist > dist_from_opt(left_ptr->get_hash_value(ctx.build_row_meta_), pos)) {
          Bucket new_bucket(salt, row_ptr);
          row_ptr->set_next(row_meta, 
                          reinterpret_cast<ObHJStoredRow *>(Bucket::END_ROW_PTR));
          set_and_shift_up(new_bucket, pos);
          break;
        }
      } else {
        ObHJStoredRow *left_ptr = reinterpret_cast<ObHJStoredRow *>(bucket->get_ptr());
        if (dist > dist_from_opt(left_ptr->get_hash_value(ctx.build_row_meta_), pos)) {
          Bucket new_bucket(salt, row_ptr);
          row_ptr->set_next(row_meta, 
                          reinterpret_cast<ObHJStoredRow *>(Bucket::END_ROW_PTR));
          set_and_shift_up(new_bucket, pos);
          break;
        }
      }
      pos = (pos + 1) & bucket_mask_;
      dist++;
      collisions++;
    }
  }
  
  return ret;
}

} // end namespace sql
} // end namespace oceanbase
