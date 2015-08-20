/*
 * Copyright 2015 Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.hops.ha.common;

import io.hops.exception.StorageException;
import io.hops.exception.StorageInitializtionException;
import io.hops.metadata.util.RMStorageFactory;
import io.hops.metadata.util.RMUtilities;
import io.hops.metadata.util.YarnAPIStorageFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.AllocateResponsePBImpl;
import org.apache.hadoop.yarn.api.records.AMCommand;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NMToken;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.PreemptionContainer;
import org.apache.hadoop.yarn.api.records.PreemptionContract;
import org.apache.hadoop.yarn.api.records.PreemptionMessage;
import org.apache.hadoop.yarn.api.records.PreemptionResourceRequest;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.StrictPreemptionContract;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationAttemptIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationIdPBImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.proto.YarnProtos;
import org.apache.hadoop.yarn.server.resourcemanager.ApplicationMasterService;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeImpl;
import org.junit.Before;
import org.junit.Test;

public class TestDBLimites {

  private static final Log LOG =
      LogFactory.getLog(TestDBLimites.class);
  
  
  @Before
  public void setup() throws IOException {
    try {
      LOG.info("Setting up Factories");
      Configuration conf = new YarnConfiguration();
      conf.set(YarnConfiguration.RM_NM_HEARTBEAT_INTERVAL_MS, "4000");
      YarnAPIStorageFactory.setConfiguration(conf);
      RMStorageFactory.setConfiguration(conf);
      RMStorageFactory.getConnector().formatStorage();
    } catch (StorageInitializtionException ex) {
      LOG.error(ex);
    } catch (StorageException ex) {
      LOG.error(ex);
    }
  }
  
  @Test
  public void TestTransactionStateCommitThroughput(){
    TransactionStateImpl ts = new TransactionStateImpl(TransactionState.TransactionType.RM);
    ApplicationId appId = ApplicationIdPBImpl.newInstance(10, 10);
    ApplicationAttemptId attId = ApplicationAttemptIdPBImpl.newInstance(appId,
            10);
    List<ContainerStatus> completedContainers = new ArrayList<ContainerStatus>();
    for(int i=0;i<1000;i++){
      ContainerId id = ContainerId.newInstance(attId, i);
      ContainerStatus status = ContainerStatus.newInstance(id,
              ContainerState.NEW, "a string", i);
      completedContainers.add(status);
    }
    List<Container> allocatedContainers = new ArrayList<Container>();
    for(int i=0; i<1000;i++){
      ContainerId id = ContainerId.newInstance(attId, i);
      NodeId nodeId = NodeId.newInstance("id", i);
      Resource r = Resource.newInstance(10, 10);
      Token containerToken = Token.newInstance(new byte[10], "kind", new byte[10], "service");
      Container c = Container.newInstance(id, nodeId, "string", r,
              Priority.UNDEFINED, containerToken);
    }
    List<NodeReport> updatedNodes = new ArrayList<NodeReport>();
    Resource availResources = Resource.newInstance(10, 10);
    AMCommand command = AMCommand.AM_RESYNC;
    int numClusterNodes = 10;
    StrictPreemptionContract set = StrictPreemptionContract.newInstance(new HashSet<PreemptionContainer>());
    PreemptionContract contract = PreemptionContract.newInstance(new ArrayList<PreemptionResourceRequest>(), new HashSet<PreemptionContainer>());
    PreemptionMessage preempt = PreemptionMessage.newInstance(set, contract);
    List<NMToken> nmTokens = new ArrayList<NMToken>();
    AllocateResponse response = AllocateResponsePBImpl.newInstance(
            10, completedContainers, allocatedContainers,
            updatedNodes, availResources, command, numClusterNodes, preempt,
            nmTokens);
    ApplicationMasterService.AllocateResponseLock lock = new ApplicationMasterService.AllocateResponseLock(response);
    ts.addAllocateResponse(attId, lock);
    
    double totalDuration = 0;
    long maxDuration =0;
    long minDuration = Integer.MAX_VALUE;
    HashMap<Long, Integer> durations = new HashMap<Long, Integer>();
    int nbruns = 10000;
    for(int i=0; i<nbruns; i++){
      long start = System.currentTimeMillis();
      RMUtilities.finishRPC(ts);
      long duration = System.currentTimeMillis() - start;
      totalDuration+=duration;
      if(duration>maxDuration) maxDuration = duration;
      if(duration<minDuration) minDuration = duration;
      if(durations.containsKey(duration)){
        durations.put(duration, durations.get(duration)+1);
      }else{
        durations.put(duration, 1);
      }
    }
    double avgDuration = totalDuration/nbruns;
    LOG.info("avgDuration = " + avgDuration + " max: " + maxDuration + " min: " + minDuration);
    LOG.info(durations.toString());
  }
}
