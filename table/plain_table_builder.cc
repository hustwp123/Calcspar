// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/plain_table_builder.h"

#include <assert.h>
#include <map>

#include "rocksdb/comparator.h"
#include "rocksdb/env.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/options.h"
#include "table/block_builder.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "table/meta_blocks.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "util/stop_watch.h"

namespace rocksdb {

namespace {

// a utility that helps writing block content to the file
//   @offset will advance if @block_contents was successfully written.
//   @block_handle the block handle this particular block.
Status WriteBlock(
    const Slice& block_contents,
    WritableFile* file,
    uint64_t* offset,
    BlockHandle* block_handle) {
  block_handle->set_offset(*offset);
  block_handle->set_size(block_contents.size());
  Status s = file->Append(block_contents);

  if (s.ok()) {
    *offset += block_contents.size();
  }
  return s;
}

}  // namespace

// kPlainTableMagicNumber was picked by running
//    echo rocksdb.plain.table | sha1sum
// and taking the leading 64 bits.
extern const uint64_t kPlainTableMagicNumber = 0x4f3418eb7a8f13b8ull;

PlainTableBuilder::PlainTableBuilder(const Options& options,
                                     WritableFile* file,
                                     uint32_t user_key_len) :
    options_(options), file_(file), user_key_len_(user_key_len) {
  properties_.fixed_key_len = user_key_len;

  // for plain table, we put all the data in a big chuck.
  properties_.num_data_blocks = 1;
  // emphasize that currently plain table doesn't have persistent index or
  // filter block.
  properties_.index_size = 0;
  properties_.filter_size = 0;
  properties_.format_version = 0;
}

PlainTableBuilder::~PlainTableBuilder() {
}

void PlainTableBuilder::Add(const Slice& key, const Slice& value) {
  assert(user_key_len_ == 0 || key.size() == user_key_len_ + 8);

  if (!IsFixedLength()) {
    // Write key length
    int key_size = key.size();
    key_size_str_.clear();
    PutVarint32(&key_size_str_, key_size);
    file_->Append(key_size_str_);
    offset_ += key_size_str_.length();
  }

  // Write key
  file_->Append(key);
  offset_ += key.size();

  // Write value length
  value_size_str_.clear();
  int value_size = value.size();
  PutVarint32(&value_size_str_, value_size);
  file_->Append(value_size_str_);

  // Write value
  file_->Append(value);
  offset_ += value_size + value_size_str_.length();

  properties_.num_entries++;
  properties_.raw_key_size += key.size();
  properties_.raw_value_size += value.size();

  // notify property collectors
  NotifyCollectTableCollectorsOnAdd(
      key,
      value,
      options_.table_properties_collectors,
      options_.info_log.get()
  );
}

Status PlainTableBuilder::status() const {
  return Status::OK();
}

Status PlainTableBuilder::Finish() {
  assert(!closed_);
  closed_ = true;

  properties_.data_size = offset_;

  // Write the following blocks
  //  1. [meta block: properties]
  //  2. [metaindex block]
  //  3. [footer]
  MetaIndexBuilder meta_index_builer;

  PropertyBlockBuilder property_block_builder;
  // -- Add basic properties
  property_block_builder.AddTableProperty(properties_);

  // -- Add user collected properties
  NotifyCollectTableCollectorsOnFinish(
      options_.table_properties_collectors,
      options_.info_log.get(),
      &property_block_builder
  );

  // -- Write property block
  BlockHandle property_block_handle;
  auto s = WriteBlock(
      property_block_builder.Finish(),
      file_,
      &offset_,
      &property_block_handle
  );
  if (!s.ok()) {
    return s;
  }
  meta_index_builer.Add(kPropertiesBlock, property_block_handle);

  // -- write metaindex block
  BlockHandle metaindex_block_handle;
  s = WriteBlock(
      meta_index_builer.Finish(),
      file_,
      &offset_,
      &metaindex_block_handle
  );
  if (!s.ok()) {
    return s;
  }

  // Write Footer
  Footer footer(kPlainTableMagicNumber);
  footer.set_metaindex_handle(metaindex_block_handle);
  footer.set_index_handle(BlockHandle::NullBlockHandle());
  std::string footer_encoding;
  footer.EncodeTo(&footer_encoding);
  s = file_->Append(footer_encoding);
  if (s.ok()) {
    offset_ += footer_encoding.size();
  }

  return s;
}

void PlainTableBuilder::Abandon() {
  closed_ = true;
}

uint64_t PlainTableBuilder::NumEntries() const {
  return properties_.num_entries;
}

uint64_t PlainTableBuilder::FileSize() const {
  return offset_;
}

}  // namespace rocksdb
