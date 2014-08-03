// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb;

/**
 * Slices are used by RocksDB to provide
 * efficient access to keys and values.
 *
 * This class is package private, implementers
 * should extend either of the public abstract classes:
 *   @see org.rocksdb.Slice
 *   @see org.rocksdb.DirectSlice
 */
abstract class AbstractSlice<T> extends RocksObject {

  /**
   * Returns the data.
   *
   * @return The data. Note, the type of access is
   *   determined by the subclass
   *   @see org.rocksdb.AbstractSlice#data0(long).
   */
  public T data() {
    assert (isInitialized());
    return data0(nativeHandle_);
  }

  /**
   * Access to the data is provided by the
   * subtype as it needs to handle the
   * generic typing.
   *
   * @param handle The address of the underlying
   *   native object.
   *
   * @return Java typed access to the data.
   */
  protected abstract T data0(long handle);

  /**
   * Return the length (in bytes) of the data.
   *
   * @return The length in bytes.
   */
  public int size() {
    assert (isInitialized());
    return size0(nativeHandle_);
  }

  /**
   * Return true if the length of the
   * data is zero.
   *
   * @return true if there is no data, false otherwise.
   */
  public boolean empty() {
    assert (isInitialized());
    return empty0(nativeHandle_);
  }

  /**
   * Creates a string representation of the data
   *
   * @param hex When true, the representation
   *   will be encoded in hexidecimal.
   *
   * @return The string representation of the data.
   */
  public String toString(final boolean hex) {
    assert (isInitialized());
    return toString0(nativeHandle_, hex);
  }

  @Override
  public String toString() {
    return toString(false);
  }

  /**
   * Three-way key comparison
   *
   *  @param other A slice to compare against
   *
   *  @return Should return either:
   *    1) < 0 if this < other
   *    2) == 0 if this == other
   *    3) > 0 if this > other
   */
  public int compare(final AbstractSlice other) {
    assert (other != null);
    assert (isInitialized());
    return compare0(nativeHandle_, other.nativeHandle_);
  }

  /**
   * If other is a slice, then
   * we defer to compare to check equality,
   * otherwise we return false.
   *
   * @param other Object to test for equality
   *
   * @return true when this.compare(other) == 0,
   *   false otherwise.
   */
  @Override
  public boolean equals(final Object other) {
    if (other != null && other instanceof AbstractSlice) {
      return compare((AbstractSlice)other) == 0;
    } else {
      return false;
    }
  }

  /**
   * Determines whether this starts with prefix
   *
   * @param prefix Another slice which may of may not
   *   be the prefix of this slice.
   *
   * @return true when slice `prefix` is a prefix
   *   of this slice
   */
  public boolean startsWith(final AbstractSlice prefix) {
    if (prefix != null) {
      assert (isInitialized());
      return startsWith0(nativeHandle_, prefix.nativeHandle_);
    } else {
      return false;
    }
  }

  /**
   * Deletes underlying C++ slice pointer.
   * <p/>
   * Note that this function should be called only after all
   * RocksDB instances referencing the slice are closed.
   * Otherwise an undefined behavior will occur.
   */
  @Override
  protected void disposeInternal() {
    assert(isInitialized());
    disposeInternal(nativeHandle_);
  }

  protected native void createNewSliceFromString(String str);
  private native int size0(long handle);
  private native boolean empty0(long handle);
  private native String toString0(long handle, boolean hex);
  private native int compare0(long handle, long otherHandle);
  private native boolean startsWith0(long handle, long otherHandle);
  private native void disposeInternal(long handle);

}
