/*
 * Copyright 2014 Apache Software Foundation.
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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair;

import io.hops.exception.StorageException;
import io.hops.exception.StorageInitializtionException;
import io.hops.metadata.util.RMStorageFactory;
import io.hops.metadata.util.RMUtilities;
import io.hops.metadata.util.YarnAPIStorageFactory;
import java.io.IOException;
import java.util.Collection;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import junit.framework.Assert;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerLaunchContextPBImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.resourcemanager.ClientRMService;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.NDBRMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplication;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.TestSchedulerRecovery;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.util.resource.Resources;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.Test;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


/**
 *
 * @author nickstanogias
 */
public class TestFairSchedulerRecovery {

  private static final Log LOG = LogFactory.getLog(TestSchedulerRecovery.class);
  private YarnConfiguration conf;
  private final int GB = 1024;
  private RecordFactory recordFactory = RecordFactoryProvider
          .getRecordFactory(null);
  private String appType = "MockApp";

  @Before
  public void setup() {
    try {
      LOG.info("Setting up Factories");
      conf = new YarnConfiguration();
      conf.set(YarnConfiguration.RM_NM_HEARTBEAT_INTERVAL_MS, "4000");

      YarnAPIStorageFactory.setConfiguration(conf);
      RMStorageFactory.setConfiguration(conf);
      RMStorageFactory.getConnector().formatStorage();

      RMUtilities.InitializeDB();
      conf.setClass(YarnConfiguration.RM_SCHEDULER,
              FairScheduler.class, ResourceScheduler.class);
    } catch (StorageInitializtionException ex) {
      LOG.error(ex);
    } catch (StorageException ex) {
      LOG.error(ex);
    } catch (IOException ex) {
      LOG.error(ex);
    }
  }

  @Test(timeout = 600000)
  public void testFairSchedulerRecovery() throws Exception {
    NDBRMStateStore stateStore = new NDBRMStateStore();
    stateStore.init(conf);
    MockRM rm = new MockRM(conf);
    rm.start();
    ClientRMService rmService = rm.getClientRMService();

    GetApplicationsRequest getRequest = GetApplicationsRequest.newInstance(EnumSet.of(YarnApplicationState.KILLED));

    ApplicationId appId1 = getApplicationId(100);
    ApplicationId appId2 = getApplicationId(101);

    ApplicationACLsManager mockAclsManager = mock(ApplicationACLsManager.class);
    when(
            mockAclsManager.checkAccess(UserGroupInformation.getCurrentUser(),
                    ApplicationAccessType.VIEW_APP, null, appId1)).thenReturn(true);

    SubmitApplicationRequest submitRequest1 = mockSubmitAppRequest(appId1, "user1", "queue1");
    SubmitApplicationRequest submitRequest2 = mockSubmitAppRequest(appId2, "user2", "queue2");

    try {
      rmService.submitApplication(submitRequest1);
      rmService.submitApplication(submitRequest2);

    } catch (YarnException e) {
      Assert.fail("Exception is not expected.");
    }

    assertEquals("Incorrect number of apps in the RM", 0, rmService.getApplications(getRequest).getApplicationList().size());
    Thread.sleep(1000);

    MockNM nm1 = rm.registerNode("host1:1234", 5120);
    MockNM nm2 = rm.registerNode("host2:5678", 10240);
    Thread.sleep(1000);

    NodeHeartbeatResponse nodeHeartbeat = nm1.nodeHeartbeat(true);
    org.junit.Assert.assertEquals(4000, nodeHeartbeat.getNextHeartBeatInterval());

    NodeHeartbeatResponse nodeHeartbeat2 = nm2.nodeHeartbeat(true);
    org.junit.Assert.assertEquals(4000, nodeHeartbeat2.getNextHeartBeatInterval());

    FairScheduler fairScheduler = (FairScheduler) rm.getResourceScheduler();
    Map<NodeId, FSSchedulerNode> beforeRecoveryNodes = fairScheduler.getNodes();
    assertEquals(2, beforeRecoveryNodes.size());

    QueueManager queueMgrBeforeRecovery = fairScheduler.getQueueManager();
    Collection<FSLeafQueue> leafQueuesBR = queueMgrBeforeRecovery.getLeafQueues();
    Collection<FSQueue> queuesBR = queueMgrBeforeRecovery.getQueues();
        //Thread.sleep(1000000);

    //close the first RM
    //rm.close();
    /**
     * ****************************************************************************************************************
     */
    /**
     * ****************************************************************************************************************
     */
    //we enable the recovery of the RM
    conf.setBoolean(YarnConfiguration.RECOVERY_ENABLED, true);
    NDBRMStateStore stateStore2 = new NDBRMStateStore();
    stateStore2.init(conf);
    MockRM rm2 = new MockRM(conf, stateStore2);
    rm2.start();
    FairScheduler scheduler = (FairScheduler) rm2.getResourceScheduler();
//        RMContext rmContext = rm2.getRMContext();
//        RMStateStore rmStateStore = rmContext.getStateStore();     
//        RMState state = rmStateStore.loadState();
//        RMAppManager rmAppManager = rm2.getRMAppManager();     
//        rmAppManager.recover(state);
    scheduler.reinitialize(conf, null);

    //check that applications are recovered properly
    Map<ApplicationId, SchedulerApplication> apps = scheduler.getSchedulerApplications();
    org.junit.Assert.assertTrue("app " + appId1.getId() + " was not recoverd",
            apps.containsKey(appId1));
    org.junit.Assert.assertTrue("app " + appId2.getId() + " was not recoverd",
            apps.containsKey(appId2));

    //check that nodes are recovered propely
    Map<NodeId, FSSchedulerNode> afterRecoveryNodes = scheduler.getNodes();
    assertEquals(2, afterRecoveryNodes.size());

    //check that queueManager is rebuilt again properly
    QueueManager queueMgrAfterRecovery = scheduler.getQueueManager();
    Collection<FSLeafQueue> leafQueuesAR = queueMgrAfterRecovery.getLeafQueues();
    Collection<FSQueue> queuesAR = queueMgrAfterRecovery.getQueues();

    Thread.sleep(1000);
  }

  private static ApplicationId getApplicationId(int id) {
    return ApplicationId.newInstance(123456, id);
  }

  private SubmitApplicationRequest mockSubmitAppRequest(ApplicationId appId,
          String name, String queue) {
    return mockSubmitAppRequest(appId, name, queue, null);
  }

  private SubmitApplicationRequest mockSubmitAppRequest(ApplicationId appId,
          String name, String queue, Set<String> tags) {
    return mockSubmitAppRequest(appId, name, queue, tags, false);
  }

  private SubmitApplicationRequest mockSubmitAppRequest(ApplicationId appId,
          String name, String queue, Set<String> tags, boolean unmanaged) {

//    ContainerLaunchContext amContainerSpec = mock(ContainerLaunchContext.class);
    ContainerLaunchContext amContainerSpec = new ContainerLaunchContextPBImpl();
    Resource resource = Resources.createResource(
            YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB);

    ApplicationSubmissionContext submissionContext = recordFactory.newRecordInstance(ApplicationSubmissionContext.class);
    submissionContext.setAMContainerSpec(amContainerSpec);
    submissionContext.setApplicationName(name);
    submissionContext.setQueue(queue);
    submissionContext.setApplicationId(appId);
    submissionContext.setResource(resource);
    submissionContext.setApplicationType(appType);
    submissionContext.setApplicationTags(tags);
    submissionContext.setUnmanagedAM(unmanaged);

//    SubmitApplicationRequest submitRequest =
//            new SubmitApplicationRequestPBImpl();
//    submitRequest.setApplicationSubmissionContext(submissionContext);
    SubmitApplicationRequest submitRequest = recordFactory.newRecordInstance(SubmitApplicationRequest.class);
    submitRequest.setApplicationSubmissionContext(submissionContext);
    return submitRequest;
  }
}
