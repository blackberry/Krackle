package com.blackberry.kafka.lowoverhead;

public enum KafkaError {
    NoError((short) 0, "No error--it worked!"), //
    Unknown((short) -1, "An unexpected server error"), //
    OffsetOutOfRange(
	    (short) 1,
	    "The requested offset is outside the range of offsets maintained by the server for the given topic/partition."), //
    InvalidMessage((short) 2,
	    "This indicates that a message contents does not match its CRC"), //
    UnknownTopicOrPartition((short) 3,
	    "This request is for a topic or partition that does not exist on this broker."), //
    InvalidMessageSize((short) 4, "The message has a negative size"), //
    LeaderNotAvailable(
	    (short) 5,
	    "This error is thrown if we are in the middle of a leadership election and there is currently no leader for this partition and hence it is unavailable for writes."), //
    NotLeaderForPartition(
	    (short) 6,
	    "This error is thrown if the client attempts to send messages to a replica that is not the leader for some partition. It indicates that the clients metadata is out of date."), //
    RequestTimedOut(
	    (short) 7,
	    "This error is thrown if the request exceeds the user-specified time limit in the request."), //
    BrokerNotAvailable(
	    (short) 8,
	    "This is not a client facing error and is used only internally by intra-cluster broker communication."), //
    ReplicaNotAvailable((short) 9,
	    "What is the difference between this and LeaderNotAvailable?"), //
    MessageSizeTooLarge(
	    (short) 10,
	    "The server has a configurable maximum message size to avoid unbounded memory allocation. This error is thrown if the client attempt to produce a message larger than this maximum."), //
    StaleControllerEpochCode((short) 11, "???"), //
    OffsetMetadataTooLargeCode((short) 12,
	    "If you specify a string larger than configured maximum for offset metadata");//

    private final short code;
    private final String message;

    KafkaError(short code, String message) {
	this.code = code;
	this.message = message;
    }

    public short getCode() {
	return code;
    }

    public String getMessage() {
	return message;
    }

    public static String getMessage(short errorCode) {
	for (KafkaError e : KafkaError.values()) {
	    if (e.getCode() == errorCode) {
		return e.getMessage();
	    }
	}
	return null;
    }

}
