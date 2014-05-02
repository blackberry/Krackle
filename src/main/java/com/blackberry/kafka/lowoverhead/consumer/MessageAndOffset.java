package com.blackberry.kafka.lowoverhead.consumer;

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
