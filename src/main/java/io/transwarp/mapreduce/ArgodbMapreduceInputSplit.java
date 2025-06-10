package io.transwarp.mapreduce;

import io.transwarp.nucleon.rdd.shiva2.CorePartition;
import org.apache.hadoop.mapreduce.InputSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;

import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class ArgodbMapreduceInputSplit extends InputSplit implements Serializable {

  private static final Logger LOGGER = LoggerFactory.getLogger(ArgodbMapreduceInputSplit.class);

  private List<CorePartition> corePartitions = new ArrayList<>(1);
  private String partDefine;

  public ArgodbMapreduceInputSplit(CorePartition corePartition, String partDefine) {
    this(corePartition);
    this.partDefine = partDefine;
  }

  public ArgodbMapreduceInputSplit(CorePartition corePartition) {
    this.corePartitions.add(corePartition);
  }

  public List<CorePartition> getCorePartitions() {
    return corePartitions;
  }

  public String getPartDefine() {
    return partDefine;
  }

  public void setPartDefine(String partDefine) {
    this.partDefine = partDefine;
  }

  @Override
  public long getLength() throws IOException {
    return 0;
  }

  @Override
  public String[] getLocations() throws IOException {
    if (corePartitions.isEmpty()) {
      return new String[0];
    }

    String[] ipArr = JavaConverters.seqAsJavaListConverter(corePartitions.get(0).getPreferredLocations()).asJava().toArray(new String[0]);
    String[] hosts = new String[ipArr.length];
    for (int i = 0; i < ipArr.length; ++i) {
      InetAddress addr = InetAddress.getByName(ipArr[i]);
      hosts[i] = addr.getHostName();
    }
    return hosts;
  }

}
