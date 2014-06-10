/**
 * Copyright 2014 BlackBerry, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.blackberry.kafka.lowoverhead.compression;

import java.io.IOException;

/**
 * Interface for compressors used to compress data for sending to the broker.
 */
public interface Compressor {
  /**
   * Return the attribute value associated with this compression method.
   * 
   * @return the attribute value associated with this compression method.
   */
  public byte getAttribute();

  /**
   * Compresses the data from the source array into the destination array.
   * 
   * If the destination array is (potentially) not big enough to hold the
   * compressed data, then the compress method will not compress anything and
   * return <code>-1</code>.
   * 
   * @param src
   *          source byte array.
   * @param srcPos
   *          start position of data in the source byte array.
   * @param length
   *          length of data in the source byte array.
   * @param dest
   *          destination byte array
   * @param destPos
   *          position in destination byte array to write to
   * @return the number of bytes written to the destination array, or
   *         <code>-1</code> if there was not enough room to write the
   *         compressed data.
   * @throws IOException
   */
  public int compress(byte[] src, int srcPos, int length, byte[] dest,
      int destPos) throws IOException;
}
