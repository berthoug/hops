/*
 * Copyright 2016 Apache Software Foundation.
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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.quota;

import io.hops.StorageConnector;
import io.hops.metadata.util.RMStorageFactory;
import io.hops.metadata.util.RMUtilities;
import io.hops.metadata.util.YarnAPIStorageFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import junit.framework.Assert;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.ContainersLogsService;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.RMContextImpl;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author rizvi
 */
public class TestPriceEstimationService {
  private static final Log LOG = LogFactory.getLog(TestPriceEstimationService.class);
  private StorageConnector connector = null;
  private Configuration conf = null;
  private final static int WAIT_SLEEP_MS = 100;
  private final int GB = 1024;
  
  @Before
  public void setup() throws IOException {
    conf = new YarnConfiguration();
    LOG.info("DFS_STORAGE_DRIVER_JAR_FILE : " + YarnAPIStorageFactory.DFS_STORAGE_DRIVER_JAR_FILE);
    LOG.info("DFS_STORAGE_DRIVER_JAR_FILE_DEFAULT : " + YarnAPIStorageFactory.DFS_STORAGE_DRIVER_JAR_FILE_DEFAULT);
    LOG.info("DFS_STORAGE_DRIVER_CLASS : " + YarnAPIStorageFactory.DFS_STORAGE_DRIVER_CLASS);
    LOG.info("DFS_STORAGE_DRIVER_CLASS_DEFAULT : " + YarnAPIStorageFactory.DFS_STORAGE_DRIVER_CLASS_DEFAULT);

    YarnAPIStorageFactory.setConfiguration(conf);
    RMStorageFactory.setConfiguration(conf);
    RMUtilities.InitializeDB();
  }
  
  @Test
  public void TestPriceEstimation() throws Exception{
  
      
      // Starting price estination service   
      conf.setInt(YarnConfiguration.QUOTAS_CONTAINERS_LOGS_MONITOR_INTERVAL,2000);
      conf.setFloat(YarnConfiguration.MAXIMUM_PERCENTAGE_OF_MEMORY_USAGE_WITH_MINIMUM_PRICE,0.2f);
      conf.setFloat(YarnConfiguration.MAXIMUM_PERCENTAGE_OF_VIRTUAL_CORE_USAGE_WITH_MINIMUM_PRICE,0.2f);
      
      conf.setFloat(YarnConfiguration.MINIMUM_PRICE_PER_TICK_FOR_MEMORY,50f);
      conf.setFloat(YarnConfiguration.MINIMUM_PRICE_PER_TICK_FOR_VIRTUAL_CORE,50f);      
    
      MockRM rm = new MockRM(conf);          
      rm.start();
      
      
      PriceEstimationService ps = new PriceEstimationService(rm.getRMContext()); // RMcontext has to be passed
      ps.init(conf);
      ps.start();
         
      
         
      // Start the nodes
      MockNM nm1 = rm.registerNode("h1:1234", 5*GB);
      MockNM nm2 = rm.registerNode("h2:5678",10*GB);

      RMApp app = rm.submitApp(1*GB);

      //kick the scheduling
      nm1.nodeHeartbeat(true);

      RMAppAttempt attempt = app.getCurrentAppAttempt();
      MockAM am = rm.sendAMLaunched(attempt.getAppAttemptId());
      am.registerAppAttempt();

      //request for containers
      int request = 4;
      am.allocate("h1", 1*GB, request, new ArrayList<ContainerId>());

      //kick the scheduler
      List<Container> conts = am.allocate(new ArrayList<ResourceRequest>(),new ArrayList<ContainerId>()).getAllocatedContainers();
      int contReceived = conts.size();
      while (contReceived < 4) {//only 4 containers can be allocated on node1
        nm1.nodeHeartbeat(true);
        conts.addAll(am.allocate(new ArrayList<ResourceRequest>(),new ArrayList<ContainerId>()).getAllocatedContainers());
        contReceived = conts.size();
        //LOG.info("Got " + contReceived + " containers. Waiting to get " + 3);
        Thread.sleep(WAIT_SLEEP_MS);
      }
      Assert.assertEquals(4, conts.size());
      
      Thread.sleep(5000);
      
      //request for containers
      request = 10;
      am.allocate("h1", 1*GB, request, new ArrayList<ContainerId>());
      //send node2 heartbeat      
      conts = am.allocate(new ArrayList<ResourceRequest>(),new ArrayList<ContainerId>()).getAllocatedContainers();
      contReceived = conts.size();
      while (contReceived < 10) {//Now rest of the (request-4=10) containers can be allocated on node1
        nm2.nodeHeartbeat(true);
        conts.addAll(am.allocate(new ArrayList<ResourceRequest>(),new ArrayList<ContainerId>()).getAllocatedContainers());
        contReceived = conts.size();
        //LOG.info("Got " + contReceived + " containers. Waiting to get " + 10);
        Thread.sleep(WAIT_SLEEP_MS );
      }
      Assert.assertEquals(10, conts.size());
       
    
    Thread.sleep(1000);  
    
  }
  
  @Test
  public void TestContainerLogServiceUpdatesPrice(){
    
    try {
      
      
      conf.setInt(YarnConfiguration.QUOTAS_CONTAINERS_LOGS_MONITOR_INTERVAL,1000);
      // When RM_HA_ENABLED is disabled the ResourceManager is itself a ResourceTracker.
      // Otherwise the ResourceManager and ResourceTracker are two different nodes.
      conf.setBoolean(YarnConfiguration.RM_HA_ENABLED, false);
      MockRM rm = new MockRM(conf);
      rm.start();
      
      
      //RMContext rmContext = new RMContextImpl();
      //ContainersLogsService logService = new ContainersLogsService(rmContext);
      
      ContainersLogsService logService = new ContainersLogsService(rm.getRMContext());
      ((RMContextImpl)rm.getRMContext()).setContainersLogsService(logService);
      logService.init(conf);
      logService.start();
      
      Thread.sleep(2000);
            
      if (rm.getRMContext().getContainersLogsService() == null )
        LOG.info("Container log service not started");
            
      PriceEstimationService ps = new PriceEstimationService(rm.getRMContext()); // RMcontext has to be passed
      ps.init(conf);
      ps.start();
      
      Thread.sleep(5000);
      
    } catch (InterruptedException ex) {
      Logger.getLogger(TestPriceEstimationService.class.getName()).log(Level.SEVERE, null, ex);
    }
    
    
  }
    
  

}
