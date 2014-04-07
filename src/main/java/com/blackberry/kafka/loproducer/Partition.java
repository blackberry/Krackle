package com.blackberry.kafka.loproducer;

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
