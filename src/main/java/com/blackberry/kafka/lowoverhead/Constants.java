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
