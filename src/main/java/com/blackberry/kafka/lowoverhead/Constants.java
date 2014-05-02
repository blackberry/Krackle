package com.blackberry.kafka.lowoverhead;

import java.nio.charset.Charset;

public class Constants {
    public static final Charset UTF8 = Charset.forName("UTF8");

    public static final byte MAGIC_BYTE = 0;
    public static final short API_VERSION = 0;

    public static final byte COMPRESSION_MASK = 0x03;

    public static final short APIKEY_PRODUCE = 0;
    public static final short APIKEY_FETCH_REQUEST = 1;
    public static final short APIKEY_OFFSET_REQUEST = 2;
    public static final short APIKEY_METADATA = 3;

    public static final byte NO_COMPRESSION = 0;
    public static final byte GZIP = 1; // gzip compression
    public static final byte SNAPPY = 2; // snappy compression

    public static final long EARLIEST_OFFSET = -2L;
    public static final long LATEST_OFFSET = -1L;
}
