package com.blackberry.kafka.lowoverhead.meta;

import java.util.ArrayList;
import java.util.List;

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
