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
import io.hops.metadata.yarn.dal.ContainerDataAccess;
import io.hops.metadata.yarn.dal.RMContainerDataAccess;
import io.hops.metadata.yarn.dal.util.YARNOperationType;
import io.hops.metadata.yarn.entity.Container;
import io.hops.metadata.yarn.entity.RMContainer;
import io.hops.transaction.handler.LightWeightRequestHandler;
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
  public void rmContainerClusterJBombing() throws IOException {
    final List<RMContainer> toAdd = new ArrayList<RMContainer>();
    for (int i = 0; i < 4000; i++) {
      RMContainer container = new RMContainer("containerid", "appAttemptId",
              "nodeId", "user", "reservedNodeId", i, i, i, i, i,
              "state", "finishedStatusState", i);
      toAdd.add(container);
    }
    for(int i= 0; i<50; i++){
      long start = System.currentTimeMillis();
      LightWeightRequestHandler bomb = new LightWeightRequestHandler(
              YARNOperationType.TEST) {
                @Override
                public Object performTask() throws IOException {
                  connector.beginTransaction();
                  connector.writeLock();
                  RMContainerDataAccess rmcontainerDA
                  = (RMContainerDataAccess) RMStorageFactory
                  .getDataAccess(RMContainerDataAccess.class);
                  rmcontainerDA.addAll(toAdd);
                  connector.commit();
                  return null;
                }
              };
      bomb.handle();
      long duration = System.currentTimeMillis() - start;
      LOG.info("duration: " + duration);
    }
  }
  
   @Test
  public void ContainerClusterJBombing() throws IOException {
    final List<Container> toAdd = new ArrayList<Container>();
    for (int i = 0; i < 3000; i++) {
      Container c = new Container("containerId", new byte[1]);
      toAdd.add(c);
    }
    for(int i= 0; i<50; i++){
      long start = System.currentTimeMillis();
      LightWeightRequestHandler bomb = new LightWeightRequestHandler(
              YARNOperationType.TEST) {
                @Override
                public Object performTask() throws IOException {
                  connector.beginTransaction();
                  connector.writeLock();
                  ContainerDataAccess containerDA
                  = (ContainerDataAccess) RMStorageFactory
                  .getDataAccess(ContainerDataAccess.class);
                  containerDA.addAll(toAdd);
                  connector.commit();
                  return null;
                }
              };
      bomb.handle();
      long duration = System.currentTimeMillis() - start;
      LOG.info("duration: " + duration);
    }
  }
}
