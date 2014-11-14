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

package com.blackberry.krackle.consumer;

/**
 * A simple class for holding a message as as byte array and an offset as a
 * long.
 */
public class MessageAndOffset {
  private byte[] message;
  private long offset;

  public MessageAndOffset() {
  }

  public MessageAndOffset(byte[] message, long offset) {
    this.message = message;
    this.offset = offset;
  }

  public byte[] getMessage() {
    return message;
  }

  public void setMessage(byte[] message) {
    this.message = message;
  }

  public long getOffset() {
    return offset;
  }

  public void setOffset(long offset) {
    this.offset = offset;
  }

}
