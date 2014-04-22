// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb;

/**
 * Filters are stored in rocksdb and are consulted automatically
 * by rocksdb to decide whether or not to read some
 * information from disk. In many cases, a filter can cut down the
 * number of disk seeks form a handful to a single disk seek per
 * DB::Get() call.
 *
 * This function a new filter policy that uses a bloom filter
 * with approximately the specified number of bits per key.
 * A good value for bitsPerKey is 10, which yields a filter
 * with ~ 1% false positive rate. 
 */
public class Filter {
  private long nativeHandle_;
    
  public Filter(int bitsPerKey) {
    newFilter(bitsPerKey);
  }
  
  public long getNativeHandle() {
    return nativeHandle_;
  }
  
  /**
   * Deletes underlying C++ filter pointer.
   */
  public synchronized void dispose() {
    if(nativeHandle_ != 0) {
      dispose0(nativeHandle_);
    }
  }

  @Override protected void finalize() {
    dispose();
  }

  private boolean isInitialized() {
    return (nativeHandle_ != 0);
  }
  
  private native void newFilter(int bitsPerKey);
  private native void dispose0(long handle);
}