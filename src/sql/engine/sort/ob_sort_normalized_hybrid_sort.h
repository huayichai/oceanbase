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

#ifndef OCEANBASE_SQL_ENGINE_SORT_SORT_NORMALIZED_HYBRID_SORT_H_
#define OCEANBASE_SQL_ENGINE_SORT_SORT_NORMALIZED_HYBRID_SORT_H_

#include "lib/container/ob_array.h"
#include "storage/blocksstable/encoding/ob_encoding_query_util.h"

namespace oceanbase
{
namespace sql
{

template <typename Store_Row>
struct Int64SortKey {
	int64_t key_0_;
	static int64_t get_key_size() { return 8; }
	// l  >  r , return postive(>0)
	// l  == r , return zero(=0)
	// l  <  r , return negative(<0)
	int64_t key_cmp(const Int64SortKey &other_key) { return key_0_ - other_key.key_0_; }
	void init(const Store_Row *row, const RowMeta &row_meta) {
		key_0_ = *reinterpret_cast<const int64_t *>(row->get_cell_payload(row_meta, 1));
	}
	TO_STRING_KV(K(key_0_));
};

template <typename Store_Row>
struct Int128SortKey {
	int64_t key_0_;
	int64_t key_1_;
	static int64_t get_key_size() { return 16; }
	int64_t key_cmp(const Int128SortKey &other_key) {
		int64_t diff1 = key_0_ - other_key.key_0_;
		int64_t diff2 = key_1_ - other_key.key_1_;
		return diff1 != 0 ? diff1 : diff2;
	}
	void init(const Store_Row *row, const RowMeta &row_meta) {
		key_0_ = *reinterpret_cast<const int64_t *>(row->get_cell_payload(row_meta, 1));
		key_1_ = *reinterpret_cast<const int64_t *>(row->get_cell_payload(row_meta, 2));
	}
	TO_STRING_KV(K(key_0_), K(key_1_));
};

template <typename Key, typename Store_Row>
struct SortingItem {
	using KeyType = Key;

	Key key_;
	Store_Row *row_ptr_;
	static int64_t get_item_size() { return Key::get_key_size() + 8; }
	void init(const Store_Row *row, const RowMeta &row_meta) {
		key_.init(row, row_meta);
		row_ptr_ = const_cast<Store_Row *>(row);
	}
	TO_STRING_KV(K(key_), KP(row_ptr_));
};

template <typename Store_Row, typename SortingItem>
class ObNormalizedHybridSort
{
	using data_ptr_t = uint8_t *;
public:
	ObNormalizedHybridSort(common::ObIArray<Store_Row *> &sort_rows, const RowMeta &row_meta,
               					 common::ObIAllocator &alloc);
	int init(common::ObIArray<Store_Row *> &sort_rows, common::ObIAllocator &alloc,
           int64_t rows_begin, int64_t rows_end, bool &can_encode);
	~ObNormalizedHybridSort() { reset(); }
	void sort(int64_t rows_begin, int64_t rows_end)
  {
		if (is_mds()) {
			radix_sort<true>(reinterpret_cast<data_ptr_t>(sorting_items_),
											 reinterpret_cast<data_ptr_t>(tmp_items_),
											 item_cnt_, 0, buckets_, false);
		} else {
			radix_sort<false>(reinterpret_cast<data_ptr_t>(sorting_items_),
												reinterpret_cast<data_ptr_t>(tmp_items_),
												item_cnt_, 0, buckets_, false);
		}
    for (int64_t i = 0; i < item_cnt_ && i < (rows_end - rows_begin); i++) {
      orig_sort_rows_.at(i + rows_begin) 
                  = reinterpret_cast<Store_Row *>(sorting_items_[i].row_ptr_);
    }
  }
  void reset();

private:
	bool is_mds();
	int prepare_sorting_items(int64_t rows_begin, int64_t rows_end);
	void insertion_sort(const data_ptr_t orig_ptr, const data_ptr_t tmp_ptr,
											const int64_t count, const int64_t offset, bool swap);
	template <bool is_msd>
	void radix_sort(const data_ptr_t orig_ptr, const data_ptr_t tmp_ptr, 
									const int64_t count, const int64_t offset,
									int64_t *locations, bool swap);

public:
	static constexpr int64_t INSERTION_SORT_THRESHOLD = 16;
	static constexpr int64_t VALUES_PER_RADIX = 256;
	static constexpr int64_t RADIX_LOCATIONS = VALUES_PER_RADIX + 1;

public:
	const RowMeta &row_meta_;
  common::ObIArray<Store_Row *> &orig_sort_rows_;
	common::ObFixedArray<SortingItem, common::ObIAllocator> items_;
  common::ObIAllocator &alloc_;
	data_ptr_t buf_;
	SortingItem *sorting_items_;
	SortingItem *tmp_items_;
	int64_t item_cnt_;
	int64_t key_size_;
	int64_t item_size_;
	int64_t *buckets_;
};

template <typename Store_Row>
using Normalized64HybridSort = ObNormalizedHybridSort<Store_Row, SortingItem<Int64SortKey<Store_Row>, Store_Row>>;

template <typename Store_Row>
using Normalized128HybridSort = ObNormalizedHybridSort<Store_Row, SortingItem<Int128SortKey<Store_Row>, Store_Row>>;

} // end namespace sql
} // end namespace oceanbase

#include "ob_sort_normalized_hybrid_sort.ipp"

#endif /* OCEANBASE_SQL_ENGINE_SORT_SORT_NORMALIZED_HYBRID_SORT_H_ */