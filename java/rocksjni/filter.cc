// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
// This file implements the "bridge" between Java and C++ for
// rocksdb::FilterPolicy.

#include <stdio.h>
#include <stdlib.h>
#include <jni.h>
#include <string>

#include "include/org_rocksdb_Filter.h"
#include "rocksjni/portal.h"
#include "rocksdb/filter_policy.h"

/*
 * Class:     org_rocksdb_Filter
 * Method:    newFilter
 * Signature: (I)V
 */
void Java_org_rocksdb_Filter_newFilter(
    JNIEnv* env, jobject jobj, jint bits_per_key) {
  const rocksdb::FilterPolicy* fp = rocksdb::NewBloomFilterPolicy(bits_per_key);
  rocksdb::FilterJni::setHandle(env, jobj, fp);
}

/*
 * Class:     org_rocksdb_Filter
 * Method:    dispose0
 * Signature: (J)V
 */
void Java_org_rocksdb_Filter_dispose0(
    JNIEnv* env, jobject jobj, jlong handle) {
  auto fp = reinterpret_cast<rocksdb::FilterPolicy*>(handle);
  delete fp;

  rocksdb::FilterJni::setHandle(env, jobj, nullptr);
}
