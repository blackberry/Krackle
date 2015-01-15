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

package com.blackberry.bdp.krackle.meta;

import java.util.ArrayList;
import java.util.List;

/**
 * Holder for information about a topic.
 */
public class Topic {
  String name;
  List<Partition> partitions = new ArrayList<Partition>();

  public Topic() {
  }

  public Topic(String name) {
    this.name = name;
  }

  public int getNumPartitions() {
    return partitions.size();
  }

  public Partition getPartition(int i) {
    for (Partition p : partitions) {
      if (p.getPartId() == i) {
        return p;
      }
    }
    return null;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public List<Partition> getPartitions() {
    return partitions;
  }

  @Override
  public String toString() {
    return "Topic [name=" + name + ", partitions=" + partitions + "]";
  }
}
