// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb.test;

import java.util.Random;
import org.rocksdb.*;

public class ReadOptionsTest {
  static {
    System.loadLibrary("rocksdbjni");
  }
  public static void main(String[] args) {
    ReadOptions opt = new ReadOptions();
    Random rand = new Random();
    { // VerifyChecksums test
      boolean boolValue = rand.nextBoolean();
      opt.setVerifyChecksums(boolValue);
      assert(opt.verifyChecksums() == boolValue);
    }

    { // FillCache test
      boolean boolValue = rand.nextBoolean();
      opt.setFillCache(boolValue);
      assert(opt.fillCache() == boolValue);
    }

    { // PrefixSeek test
      boolean boolValue = rand.nextBoolean();
      opt.setPrefixSeek(boolValue);
      assert(opt.prefixSeek() == boolValue);
    }

    { // Tailing test
      boolean boolValue = rand.nextBoolean();
      opt.setTailing(boolValue);
      assert(opt.tailing() == boolValue);
    }

    opt.dispose();
    System.out.println("Passed ReadOptionsTest");
  }
}
