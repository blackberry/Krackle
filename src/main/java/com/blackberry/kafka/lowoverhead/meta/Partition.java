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

package com.blackberry.kafka.lowoverhead.meta;

import java.util.HashSet;
import java.util.Set;

public class Partition {
  int partId;
  int leader;

  Set<Integer> replicas = new HashSet<Integer>();
  Set<Integer> inSyncReplicas = new HashSet<Integer>();

  public Partition() {
  }

  public Partition(int partId) {
    this.partId = partId;
  }

  public int getPartId() {
    return partId;
  }

  public void setPartId(int partId) {
    this.partId = partId;
  }

  public int getLeader() {
    return leader;
  }

  public void setLeader(int leader) {
    this.leader = leader;
  }

  public Set<Integer> getReplicas() {
    return replicas;
  }

  public Set<Integer> getInSyncReplicas() {
    return inSyncReplicas;
  }

  @Override
  public String toString() {
    return "Partition [partId=" + partId + ", leader=" + leader + ", replicas="
        + replicas + ", inSyncReplicas=" + inSyncReplicas + "]";
  }

}
