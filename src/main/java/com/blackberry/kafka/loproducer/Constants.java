package com.blackberry.kafka.loproducer;

import java.nio.charset.Charset;

public class Constants {
  public static final Charset UTF8 = Charset.forName("UTF8");

  public static final byte MAGIC_BYTE = 0;
  public static final short API_VERSION = 0;

  public static final short APIKEY_PRODUCE = 0;
  public static final short APIKEY_METADATA = 3;

  public static final byte ATTR_NO_COMPRESSION = 0;
  public static final byte ATTR_GZIP = 1; // gzip compression
  public static final byte ATTR_SNAPPY = 2; // snappy compression

}
