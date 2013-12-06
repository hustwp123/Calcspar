//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "rocksdb/table_properties.h"

namespace rocksdb {

namespace {
  void AppendProperty(
      std::string& props,
      const std::string& key,
      const std::string& value,
      const std::string& prop_delim,
      const std::string& kv_delim) {
    props.append(key);
    props.append(kv_delim);
    props.append(value);
    props.append(prop_delim);
  }

  template <class TValue>
  void AppendProperty(
      std::string& props,
      const std::string& key,
      const TValue& value,
      const std::string& prop_delim,
      const std::string& kv_delim) {
    AppendProperty(
        props, key, std::to_string(value), prop_delim, kv_delim
    );
  }
}

std::string TableProperties::ToString(
    const std::string& prop_delim,
    const std::string& kv_delim) const {
  std::string result;
  result.reserve(1024);

  // Basic Info
  AppendProperty(
      result, "# data blocks", num_data_blocks, prop_delim, kv_delim
  );
  AppendProperty(result, "# entries", num_entries, prop_delim, kv_delim);

  AppendProperty(result, "raw key size", raw_key_size, prop_delim, kv_delim);
  AppendProperty(
      result,
      "raw average key size",
      num_entries != 0 ?  1.0 * raw_key_size / num_entries : 0.0,
      prop_delim,
      kv_delim
  );
  AppendProperty(
      result, "raw value size", raw_value_size, prop_delim, kv_delim
  );
  AppendProperty(
      result,
      "raw average value size",
      num_entries != 0 ?  1.0 * raw_value_size / num_entries : 0.0,
      prop_delim,
      kv_delim
  );

  AppendProperty(result, "data block size", data_size, prop_delim, kv_delim);
  AppendProperty(result, "index block size", index_size, prop_delim, kv_delim);
  AppendProperty(
      result, "filter block size", filter_size, prop_delim, kv_delim
  );
  AppendProperty(
      result,
      "(estimated) table size",
      data_size + index_size + filter_size,
      prop_delim,
      kv_delim
  );

  AppendProperty(
      result,
      "filter policy name",
      filter_policy_name.empty() ? std::string("N/A") : filter_policy_name,
      prop_delim,
      kv_delim
  );

  return result;
}

const std::string TablePropertiesNames::kDataSize  =
    "rocksdb.data.size";
const std::string TablePropertiesNames::kIndexSize =
    "rocksdb.index.size";
const std::string TablePropertiesNames::kFilterSize =
    "rocksdb.filter.size";
const std::string TablePropertiesNames::kRawKeySize =
    "rocksdb.raw.key.size";
const std::string TablePropertiesNames::kRawValueSize =
    "rocksdb.raw.value.size";
const std::string TablePropertiesNames::kNumDataBlocks =
    "rocksdb.num.data.blocks";
const std::string TablePropertiesNames::kNumEntries =
    "rocksdb.num.entries";
const std::string TablePropertiesNames::kFilterPolicy =
    "rocksdb.filter.policy";

extern const std::string kPropertiesBlock = "rocksdb.properties";

}  // namespace rocksdb
