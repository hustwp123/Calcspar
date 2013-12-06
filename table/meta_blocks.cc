//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "table/meta_blocks.h"

#include <map>

#include "rocksdb/table_properties.h"
#include "table/block.h"
#include "table/format.h"
#include "util/coding.h"

namespace rocksdb {

MetaIndexBuilder::MetaIndexBuilder()
    : meta_index_block_(
        new BlockBuilder(1 /* restart interval */, BytewiseComparator())) {
}

void MetaIndexBuilder::Add(const std::string& key,
                           const BlockHandle& handle) {
  std::string handle_encoding;
  handle.EncodeTo(&handle_encoding);
  meta_block_handles_.insert({key, handle_encoding});
}

Slice MetaIndexBuilder::Finish() {
  for (const auto& metablock : meta_block_handles_) {
    meta_index_block_->Add(metablock.first, metablock.second);
  }
  return meta_index_block_->Finish();
}

PropertyBlockBuilder::PropertyBlockBuilder()
  : properties_block_(
      new BlockBuilder(1 /* restart interval */, BytewiseComparator())) {
}

void PropertyBlockBuilder::Add(const std::string& name,
                               const std::string& val) {
  props_.insert({name, val});
}

void PropertyBlockBuilder::Add(const std::string& name, uint64_t val) {
  assert(props_.find(name) == props_.end());

  std::string dst;
  PutVarint64(&dst, val);

  Add(name, dst);
}

void PropertyBlockBuilder::Add(
    const UserCollectedProperties& user_collected_properties) {
  for (const auto& prop : user_collected_properties) {
    Add(prop.first, prop.second);
  }
}

void PropertyBlockBuilder::AddTableProperty(const TableProperties& props) {
  Add(TablePropertiesNames::kRawKeySize, props.raw_key_size);
  Add(TablePropertiesNames::kRawValueSize, props.raw_value_size);
  Add(TablePropertiesNames::kDataSize, props.data_size);
  Add(TablePropertiesNames::kIndexSize, props.index_size);
  Add(TablePropertiesNames::kNumEntries, props.num_entries);
  Add(TablePropertiesNames::kNumDataBlocks, props.num_data_blocks);
  Add(TablePropertiesNames::kFilterSize, props.filter_size);

  if (!props.filter_policy_name.empty()) {
    Add(TablePropertiesNames::kFilterPolicy,
        props.filter_policy_name);
  }
}

Slice PropertyBlockBuilder::Finish() {
  for (const auto& prop : props_) {
    properties_block_->Add(prop.first, prop.second);
  }

  return properties_block_->Finish();
}

void LogPropertiesCollectionError(
    Logger* info_log, const std::string& method, const std::string& name) {
  assert(method == "Add" || method == "Finish");

  std::string msg =
    "[Warning] encountered error when calling TablePropertiesCollector::" +
    method + "() with collector name: " + name;
  Log(info_log, "%s", msg.c_str());
}

bool NotifyCollectTableCollectorsOnAdd(
    const Slice& key,
    const Slice& value,
    const Options::TablePropertiesCollectors& collectors,
    Logger* info_log) {
  bool all_succeeded = true;
  for (auto collector : collectors) {
    Status s = collector->Add(key, value);
    all_succeeded = all_succeeded && s.ok();
    if (!s.ok()) {
      LogPropertiesCollectionError(
          info_log, "Add", /* method */ collector->Name()
      );
    }
  }
  return all_succeeded;
}

bool NotifyCollectTableCollectorsOnFinish(
    const Options::TablePropertiesCollectors& collectors,
    Logger* info_log,
    PropertyBlockBuilder* builder) {
  bool all_succeeded = true;
  for (auto collector : collectors) {
    UserCollectedProperties user_collected_properties;
    Status s = collector->Finish(&user_collected_properties);

    all_succeeded = all_succeeded && s.ok();
    if (!s.ok()) {
      LogPropertiesCollectionError(
          info_log, "Finish", /* method */ collector->Name()
      );
    } else {
      builder->Add(user_collected_properties);
    }
  }

  return all_succeeded;
}

Status ReadProperties(
    const Slice& handle_value,
    RandomAccessFile* file,
    Env* env,
    Logger* logger,
    TableProperties* table_properties) {
  assert(table_properties);

  Slice v = handle_value;
  BlockHandle handle;
  if (!handle.DecodeFrom(&v).ok()) {
    return Status::InvalidArgument("Failed to decode properties block handle");
  }

  BlockContents block_contents;
  Status s = ReadBlockContents(
      file,
      ReadOptions(),
      handle,
      &block_contents,
      env,
      false
  );

  if (!s.ok()) {
    return s;
  }

  Block properties_block(block_contents);
  std::unique_ptr<Iterator> iter(
      properties_block.NewIterator(BytewiseComparator())
  );

  // All pre-defined properties of type uint64_t
  std::unordered_map<std::string, uint64_t*> predefined_uint64_properties = {
    { TablePropertiesNames::kDataSize, &table_properties->data_size },
    { TablePropertiesNames::kIndexSize, &table_properties->index_size },
    { TablePropertiesNames::kFilterSize, &table_properties->filter_size },
    { TablePropertiesNames::kRawKeySize, &table_properties->raw_key_size },
    { TablePropertiesNames::kRawValueSize, &table_properties->raw_value_size },
    { TablePropertiesNames::kNumDataBlocks,
      &table_properties->num_data_blocks },
    { TablePropertiesNames::kNumEntries, &table_properties->num_entries },
  };

  std::string last_key;
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    s = iter->status();
    if (!s.ok()) {
      break;
    }

    auto key = iter->key().ToString();
    // properties block is strictly sorted with no duplicate key.
    assert(
        last_key.empty() ||
        BytewiseComparator()->Compare(key, last_key) > 0
    );
    last_key = key;

    auto raw_val = iter->value();
    auto pos = predefined_uint64_properties.find(key);

    if (pos != predefined_uint64_properties.end()) {
      // handle predefined rocksdb properties
      uint64_t val;
      if (!GetVarint64(&raw_val, &val)) {
        // skip malformed value
        auto error_msg =
          "[Warning] detect malformed value in properties meta-block:"
          "\tkey: " + key + "\tval: " + raw_val.ToString();
        Log(logger, "%s", error_msg.c_str());
        continue;
      }
      *(pos->second) = val;
    } else if (key == TablePropertiesNames::kFilterPolicy) {
      table_properties->filter_policy_name = raw_val.ToString();
    } else {
      // handle user-collected properties
      table_properties->user_collected_properties.insert(
          std::make_pair(key, raw_val.ToString())
      );
    }
  }

  return s;
}

Status ReadTableProperties(
    RandomAccessFile* file,
    uint64_t file_size,
    uint64_t table_magic_number,
    Env* env,
    Logger* info_log,
    TableProperties* properties) {
  // -- Read metaindex block
  Footer footer(table_magic_number);
  auto s = ReadFooterFromFile(file, file_size, &footer);
  if (!s.ok()) {
    return s;
  }

  auto metaindex_handle = footer.metaindex_handle();
  BlockContents metaindex_contents;
  s = ReadBlockContents(
      file,
      ReadOptions(),
      metaindex_handle,
      &metaindex_contents,
      env,
      false
  );
  if (!s.ok()) {
    return s;
  }
  Block metaindex_block(metaindex_contents);
  std::unique_ptr<Iterator> meta_iter(
      metaindex_block.NewIterator(BytewiseComparator())
  );

  // -- Read property block
  meta_iter->Seek(kPropertiesBlock);
  TableProperties table_properties;
  if (meta_iter->Valid() &&
      meta_iter->key() == kPropertiesBlock &&
      meta_iter->status().ok()) {
    s = ReadProperties(
        meta_iter->value(),
        file,
        env,
        info_log,
        properties
    );
  } else {
    s = Status::Corruption(
        "Unable to read the property block from the plain table"
    );
  }

  return s;
}


}  // namespace rocksdb
