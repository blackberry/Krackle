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

package com.blackberry.bdp.krackle;

/**
 * Enum of possible errors and error codes that could be returned by the broker.
 */
public enum KafkaError {
  NoError(
      (short) 0,
      "No error--it worked!"
  ),
  Unknown(
      (short) -1,
      "An unexpected server error"
  ),
  OffsetOutOfRange(
      (short) 1,
      "The requested offset is outside the range of offsets maintained by "
      + "the server for the given topic/partition."
  ),
  InvalidMessage(
      (short) 2,
      "This indicates that a message contents does not match its CRC"
  ),
  UnknownTopicOrPartition(
      (short) 3,
      "This request is for a topic or partition that does not exist on this broker."
  ),
  InvalidMessageSize(
      (short) 4,
      "The message has a negative size"
  ),
  LeaderNotAvailable(
      (short) 5,
      "This error is thrown if we are in the middle of a leadership election "
      + "and there is currently no leader for this partition and hence it is "
      + "unavailable for writes."
  ),
  NotLeaderForPartition(
      (short) 6,
      "This error is thrown if the client attempts to send messages to a "
      + "replica that is not the leader for some partition. It indicates that "
      + "the clients metadata is out of date."
  ),
  RequestTimedOut(
      (short) 7,
      "This error is thrown if the request exceeds the user-specified time limit in the request."
  ),
  BrokerNotAvailable(
      (short) 8,
      "This is not a client facing error and is used only internally by "
      + "intra-cluster broker communication."
  ),
  Unused(
      (short) 9,
      "Unused"
  ),
  MessageSizeTooLarge(
      (short) 10,
      "The server has a configurable maximum message size to avoid unbounded "
      + "memory allocation. This error is thrown if the client attempt to "
      + "produce a message larger than this maximum."
  ),
  StaleControllerEpochCode(
      (short) 11,
      "Internal error code for broker-to-broker communication."
  ),
  OffsetMetadataTooLargeCode(
      (short) 12,
      "If you specify a string larger than configured maximum for offset metadata"
  ),
  OffsetsLoadInProgressCode(
      (short) 14,
      "The broker returns this error code for an offset fetch request if it "
      + "is still loading offsets (after a leader change for that offsets "
      + "topic partition"
  ),
  ConsumerCoordinatorNotAvailableCode(
      (short) 15,
      "The broker returns this error code for consumer metadata requests or "
      + "offset commit requests if the offsets topic has not yet been created."
  ),
  NotCoordinatorForConsumerCode(
      (short) 16,
      "The broker returns this error code if it receives an offset fetch or "
      + "commit request for a consumer group that it is not a coordinator for."
  );

  private final short code;
  private final String message;

  private KafkaError(short code, String message) {
    this.code = code;
    this.message = message;
  }

  /**
   * Returns the numerical error code for the error.
   * 
   * @return the error code.
   */
  public short getCode() {
    return code;
  }

  /**
   * Returns the message associated with the error.
   * 
   * @return the error message.
   */
  public String getMessage() {
    return message;
  }

  /**
   * Gets the message for a given error code.
   * 
   * @param errorCode
   *          a numerical error code
   * @return the error message associated with the error code
   */
  public static String getMessage(short errorCode) {
    for (KafkaError e : KafkaError.values()) {
      if (e.getCode() == errorCode) {
        return e.getMessage();
      }
    }
    return null;
  }

}
