/**
 * Copyright 2014 BlackBerry, Limited.
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

package com.blackberry.krackle.compression;

import java.io.IOException;

/**
 * Interface for decompressors used to compress data for sending to the broker.
 */
public interface Decompressor {
  /**
   * Return the attribute value associated with this compression method.
   * 
   * @return the attribute value associated with this compression method.
   */
  public byte getAttribute();

  /**
   * Decompresses the data from the source array into the destination array.
   * 
   * If the destination array is (potentially) not big enough to hold the
   * decompressed data, then the decompress method will not decompress anything
   * and return <code>-1</code>.
   * 
   * @param src
   *          source byte array.
   * @param srcPos
   *          position in source byte array to start from.
   * @param length
   *          length of data to decompress.
   * @param dest
   *          destination byte array.
   * @param destPos
   *          position in destination byte array to write to.
   * @param maxLength
   *          max length of decompressed data.
   * @return the number of bytes written to the destination, or <code>-1</code>
   *         if maxLength was not big enough to hold the data.
   * @throws IOException
   */
  public int decompress(byte[] src, int srcPos, int length, byte[] dest,
      int destPos, int maxLength) throws IOException;
}
