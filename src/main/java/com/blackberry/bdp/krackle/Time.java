/**
 * Copyright 2015 BlackBerry, Limited.
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

/**
 * This class was copied from the Apache ZooKeeper project:
 *
 * git@github.com:apache/zookeeper.git
 * 92707a6a84a7965df2d7d8ead0acb721b6e62878
 *
 * Adapted as required to work with Krackle and it's implementation
 * of AuthenticatedSocketBuilder where needed.  The following
 * major changes were performed:
 *
 * 1) Package name was changed
 * 2) Missing JavaDoc added
 *
 */

package com.blackberry.bdp.krackle;

import java.util.Date;

public class Time {
    /**
     * Returns time in milliseconds as does System.currentTimeMillis(),
     * but uses elapsed time from an arbitrary epoch more like System.nanoTime().
     * The difference is that if somebody changes the system clock,
     * Time.currentElapsedTime will change but nanoTime won't. On the other hand,
     * all of ZK assumes that time is measured in milliseconds.
     * @return  The time in milliseconds from some arbitrary point in time.
     */
    public static long currentElapsedTime() {
        return System.nanoTime() / 1000000;
    }

    /**
     * Explicitly returns system dependent current wall time.
     * @return Current time in msec.
     */
    public static long currentWallTime() {
        return System.currentTimeMillis();
    }

    /**
     * This is to convert the elapsedTime to a Date.
	 * @param elapsedTime
     * @return A date object indicated by the elapsedTime.
     */
    public static Date elapsedTimeToDate(long elapsedTime) {
        long wallTime = currentWallTime() + elapsedTime - currentElapsedTime();
        return new Date(wallTime);
    }
}