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

namespace oceanbase
{
namespace sql
{

template <typename Store_Row, typename SortingItem>
ObNormalizedHybridSort<Store_Row, SortingItem>::ObNormalizedHybridSort(
	common::ObIArray<Store_Row *> &sort_rows, const RowMeta &row_meta,
	common::ObIAllocator &alloc)
		: row_meta_(row_meta), orig_sort_rows_(sort_rows), alloc_(alloc),
			buf_(nullptr), sorting_items_(nullptr), tmp_items_(nullptr),
			item_cnt_(0), key_size_(0), item_size_(0), buckets_(nullptr)
{}

template <typename Store_Row, typename SortingItem>
int ObNormalizedHybridSort<Store_Row, SortingItem>::init(common::ObIArray<Store_Row *> &sort_rows, 
																									 		 	 common::ObIAllocator &alloc,
																									  		 int64_t rows_begin, int64_t rows_end, 
																									 			 bool &can_encode)
{
	int ret = OB_SUCCESS;
  can_encode = true;
	if (rows_end - rows_begin <= 0) {
		// do nothing
	} else if (rows_begin < 0 || rows_end > sort_rows.count()) {
		ret = OB_INVALID_ARGUMENT;
    SQL_ENG_LOG(WARN, "invalid argument", K(rows_begin), K(rows_end), K(sort_rows.count()), K(ret));
	} else if (OB_FAIL(prepare_sorting_items(rows_begin, rows_end))) {
		SQL_ENG_LOG(WARN, "failed to prepare items", K(ret));
	} else {
		for (int64_t i = 0; can_encode && i < item_cnt_; i++) {
      SortingItem &item = sorting_items_[i];
      Store_Row *row = sort_rows.at(i + rows_begin);
      if (row->is_null(0)) {
        can_encode = false;
        break;
      }
			item.init(row, row_meta_);
    }
	}
	return ret;
}

template <typename Store_Row, typename SortingItem>
int ObNormalizedHybridSort<Store_Row, SortingItem>::prepare_sorting_items(int64_t rows_begin, 
																																					int64_t rows_end)
{
	int ret = OB_SUCCESS;
	key_size_ = SortingItem::KeyType::get_key_size();
	item_size_ = SortingItem::get_item_size();
	item_cnt_ = rows_end - rows_begin;
	SQL_ENG_LOG(DEBUG, "prepare sorting items", K(key_size_), K(item_size_), K(item_cnt_));
	int64_t items_size = item_size_ * item_cnt_;
	int64_t buckets_size = RADIX_LOCATIONS * sizeof(int64_t) * key_size_;
	buf_ = reinterpret_cast<data_ptr_t>(alloc_.alloc(items_size * 2 + buckets_size));
	if (OB_ISNULL(buf_)) {
		ret = OB_ALLOCATE_MEMORY_FAILED;
		SQL_ENG_LOG(WARN, "failed to alloc memory", K(ret));
	} else {
		memset(buf_, 0, items_size * 2 + buckets_size);
		sorting_items_ = reinterpret_cast<SortingItem *>(buf_);
		tmp_items_ = reinterpret_cast<SortingItem *>(buf_ + items_size);
		buckets_ = reinterpret_cast<int64_t *>(buf_ + items_size * 2);
	}
	return ret;
}

template <typename Store_Row, typename SortingItem>
void ObNormalizedHybridSort<Store_Row, SortingItem>::reset()
{
	if (buf_ != nullptr) {
		alloc_.free(buf_);
		buf_ = nullptr;
		sorting_items_ = nullptr;
		tmp_items_ = nullptr;
	}
	item_cnt_ = 0;
	key_size_ = 0;
	item_size_ = 0;
}

template <typename Store_Row, typename SortingItem>
bool ObNormalizedHybridSort<Store_Row, SortingItem>::is_mds()
{
	int64_t val = 1;
	// &val 0 1 2 3 4 5 6 7  idx
	// LSD  1 0 0 0 0 0 0 0  
	// MSD  0 0 0 0 0 0 0 1
	return *reinterpret_cast<uint8_t *>(&val) == 0;
}

template <typename Store_Row, typename SortingItem>
void ObNormalizedHybridSort<Store_Row, SortingItem>::insertion_sort(const data_ptr_t orig_ptr, const data_ptr_t tmp_ptr,
																																		const int64_t count, const int64_t offset,
																																		bool swap)
{
	int64_t l = reinterpret_cast<SortingItem *>(orig_ptr) - sorting_items_;
	int64_t r = l + count;
	SQL_ENG_LOG(DEBUG, "insertion sort", K(l), K(r), K(count), K(offset), K(swap));
	const data_ptr_t source_ptr = swap ? tmp_ptr : orig_ptr;
	const data_ptr_t target_ptr = swap ? orig_ptr : tmp_ptr;
  if (count > 1) {
		SortingItem insert_item;
		for (int64_t i = 1; i < count; i++) {
			insert_item = *reinterpret_cast<SortingItem *>(source_ptr + i * item_size_);
			int64_t j = i;
			while (j > 0) {
				SortingItem &cur_item = *reinterpret_cast<SortingItem *>(source_ptr + (j - 1) * item_size_);
				if (cur_item.key_.key_cmp(insert_item.key_) > 0) {
					memcpy(source_ptr + j * item_size_, source_ptr + (j - 1) * item_size_, item_size_);
					j--;
				} else {
					break;
				}
			}
			memcpy(source_ptr + j * item_size_, reinterpret_cast<data_ptr_t>(&insert_item), item_size_);
		}
	}
	if (swap) {
		memcpy(target_ptr, source_ptr, count * item_size_);
	}
}

template <typename Store_Row, typename SortingItem>
template <bool is_msd>
void ObNormalizedHybridSort<Store_Row, SortingItem>::radix_sort(const data_ptr_t orig_ptr, const data_ptr_t tmp_ptr, 
																																const int64_t count, const int64_t offset,
																																int64_t *locations, bool swap)
{
	int64_t l = reinterpret_cast<SortingItem *>(orig_ptr) - sorting_items_;
	int64_t r = l + count;
	SQL_ENG_LOG(DEBUG, "radix sort", K(l), K(r), K(count), K(offset), K(swap));

	int64_t real_offset;
	if (is_msd) {
		real_offset = offset;
	} else {
		int64_t cmped_cnt = offset / 8;
		real_offset = (cmped_cnt * 8) + (((cmped_cnt + 1) * 8 - 1) - offset);
	}

	const data_ptr_t source_ptr = swap ? tmp_ptr : orig_ptr;
	const data_ptr_t target_ptr = swap ? orig_ptr : tmp_ptr;
	// init counts to 0
	memset(locations, 0, RADIX_LOCATIONS * sizeof(int64_t));
	int64_t *counts = locations + 1;
	// collect counts
	data_ptr_t offset_ptr = source_ptr + real_offset;
	for (int64_t i = 0; i < count; i++) {
		counts[*offset_ptr]++;
		offset_ptr += item_size_;
	}
	// compute locations from buckets
	int64_t max_count = 0;
	for (int64_t radix = 0; radix < VALUES_PER_RADIX; radix++) {
		max_count = std::max(max_count, counts[radix]);
		counts[radix] += locations[radix];
	}
	if (max_count != count) {
		// reorder items into tmp array
		data_ptr_t item_ptr = source_ptr;
		for (int64_t i = 0; i < count; i++) {
			const int64_t &radix_offset = locations[*(item_ptr + real_offset)]++;
			memcpy(target_ptr + radix_offset * item_size_, item_ptr, item_size_);
			item_ptr += item_size_;
		}
		swap = !swap;
	}
	// check if done
	if (offset == key_size_ - 1) {
		if (swap) {
			memcpy(orig_ptr, tmp_ptr, count * item_size_);
		}
	} else if (max_count == count) {
		radix_sort<is_msd>(orig_ptr, tmp_ptr, count, offset + 1, locations + RADIX_LOCATIONS, swap);
	} else {
		// recurse
		int64_t radix_count = locations[0];
		for (int64_t radix = 0; radix < VALUES_PER_RADIX; radix++) {
			const int64_t loc = (locations[radix] - radix_count) * item_size_;
			if (radix_count > INSERTION_SORT_THRESHOLD) {
				radix_sort<is_msd>(orig_ptr + loc, tmp_ptr + loc, radix_count, offset + 1, 
									 locations + RADIX_LOCATIONS, swap);
			} else if (radix_count != 0) {
				insertion_sort(orig_ptr + loc, tmp_ptr + loc, radix_count, offset + 1, swap);
			}
			radix_count = locations[radix + 1] - locations[radix];
		}
	}
}

} // namespace sql
} // namespace oceanbase