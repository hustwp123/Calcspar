// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
// This file implements the "bridge" between Java and C++ and enables
// calling c++ rocksdb::Iterator methods from Java side.

#include <stdio.h>
#include <stdlib.h>
#include <jni.h>

#include "include/org_rocksdb_Iterator.h"
#include "rocksjni/portal.h"
#include "rocksdb/iterator.h"

/*
 * Class:     org_rocksdb_Iterator
 * Method:    isValid0
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Iterator_isValid0(
    JNIEnv* env, jobject jobj, jlong handle) {
  auto it = rocksdb::IteratorJni::getIterator(handle);
  return it->Valid();
}

/*
 * Class:     org_rocksdb_Iterator
 * Method:    close0
 * Signature: (J)V
 */
void Java_org_rocksdb_Iterator_seekToFirst0(
    JNIEnv* env, jobject jobj, jlong handle) {
  auto it = rocksdb::IteratorJni::getIterator(handle);
  it->SeekToFirst();
}

/*
 * Class:     org_rocksdb_Iterator
 * Method:    seekToFirst0
 * Signature: (J)V
 */
void Java_org_rocksdb_Iterator_seekToLast0(
    JNIEnv* env, jobject jobj, jlong handle) {
  auto it = rocksdb::IteratorJni::getIterator(handle);
  it->SeekToLast();
}

/*
 * Class:     org_rocksdb_Iterator
 * Method:    seekToLast0
 * Signature: (J)V
 */
void Java_org_rocksdb_Iterator_next0(
    JNIEnv* env, jobject jobj, jlong handle) {
  auto it = rocksdb::IteratorJni::getIterator(handle);
  it->Next();
}

/*
 * Class:     org_rocksdb_Iterator
 * Method:    next0
 * Signature: (J)V
 */
void Java_org_rocksdb_Iterator_prev0(
    JNIEnv* env, jobject jobj, jlong handle) {
  auto it = rocksdb::IteratorJni::getIterator(handle);
  it->Prev();
}

/*
 * Class:     org_rocksdb_Iterator
 * Method:    prev0
 * Signature: (J)V
 */
jbyteArray Java_org_rocksdb_Iterator_key0(
    JNIEnv* env, jobject jobj, jlong handle) {
  auto it = rocksdb::IteratorJni::getIterator(handle);
  rocksdb::Slice key_slice = it->key();

  jbyteArray jkey = env->NewByteArray(key_slice.size());
  env->SetByteArrayRegion(
      jkey, 0, key_slice.size(),
      reinterpret_cast<const jbyte*>(key_slice.data()));
  return jkey;
}

/*
 * Class:     org_rocksdb_Iterator
 * Method:    key0
 * Signature: (J)[B
 */
jbyteArray Java_org_rocksdb_Iterator_value0(
    JNIEnv* env, jobject jobj, jlong handle) {
  auto it = rocksdb::IteratorJni::getIterator(handle);
  rocksdb::Slice value_slice = it->value();

  jbyteArray jvalue = env->NewByteArray(value_slice.size());
  env->SetByteArrayRegion(
      jvalue, 0, value_slice.size(),
      reinterpret_cast<const jbyte*>(value_slice.data()));
  return jvalue;
}

/*
 * Class:     org_rocksdb_Iterator
 * Method:    value0
 * Signature: (J)[B
 */
void Java_org_rocksdb_Iterator_seek0(
    JNIEnv* env, jobject jobj, jlong handle,
    jbyteArray jtarget, jint jtarget_len) {
  auto it = rocksdb::IteratorJni::getIterator(handle);
  jbyte* target = env->GetByteArrayElements(jtarget, 0);
  rocksdb::Slice target_slice(
      reinterpret_cast<char*>(target), jtarget_len);

  it->Seek(target_slice);

  env->ReleaseByteArrayElements(jtarget, target, JNI_ABORT);
}

/*
 * Class:     org_rocksdb_Iterator
 * Method:    seek0
 * Signature: (J[BI)V
 */
void Java_org_rocksdb_Iterator_status0(
    JNIEnv* env, jobject jobj, jlong handle) {
  auto it = rocksdb::IteratorJni::getIterator(handle);
  rocksdb::Status s = it->status();

  if (s.ok()) {
    return;
  }

  rocksdb::RocksDBExceptionJni::ThrowNew(env, s);
}

/*
 * Class:     org_rocksdb_Iterator
 * Method:    status0
 * Signature: (J)V
 */
void Java_org_rocksdb_Iterator_close0(
    JNIEnv* env, jobject jobj, jlong handle) {
  auto it = rocksdb::IteratorJni::getIterator(handle);
  delete it;

  rocksdb::IteratorJni::setHandle(env, jobj, nullptr);
}
