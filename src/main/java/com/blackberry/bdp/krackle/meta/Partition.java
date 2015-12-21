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

import java.util.HashSet;
import java.util.Set;

/**
 * Holder for information about a partition.
 */
public class Partition {

	int partitionId;
	Broker leader;

	Set<Broker> replicas = new HashSet<>();
	Set<Broker> inSyncReplicas = new HashSet<>();

	public Partition() {
	}

	public Partition(int partId) {
		this.partitionId = partId;
	}

	public int getPartitionId() {
		return partitionId;
	}

	public void setPartitionId(int partId) {
		this.partitionId = partId;
	}

	public Broker getLeader() {
		return leader;
	}

	public void setLeader(Broker leader) {
		this.leader = leader;
	}

	public Set<Broker> getReplicas() {
		return replicas;
	}

	public Set<Broker> getInSyncReplicas() {
		return inSyncReplicas;
	}

	@Override
	public String toString() {
		return "Partition [partId=" + partitionId + ", leader=" + leader + ", replicas="
			 + replicas + ", inSyncReplicas=" + inSyncReplicas + "]";
	}

}
