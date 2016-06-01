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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.quota;

import io.hops.StorageConnector;
import io.hops.exception.StorageException;
import io.hops.exception.StorageInitializtionException;
import io.hops.metadata.util.RMStorageFactory;
import io.hops.metadata.util.RMUtilities;
import io.hops.metadata.util.YarnAPIStorageFactory;
import io.hops.metadata.yarn.dal.RMNodeDataAccess;
import io.hops.metadata.yarn.dal.ContainersLogsDataAccess;
import io.hops.metadata.yarn.dal.PendingEventDataAccess;
import io.hops.metadata.yarn.dal.YarnApplicationsToKillDataAccess;
import io.hops.metadata.yarn.dal.YarnProjectsDailyCostDataAccess;
import io.hops.metadata.yarn.dal.YarnProjectsQuotaDataAccess;
import io.hops.metadata.yarn.dal.rmstatestore.ApplicationStateDataAccess;
import io.hops.metadata.yarn.dal.util.YARNOperationType;
import io.hops.metadata.yarn.entity.RMNode;
import io.hops.metadata.yarn.entity.ContainersLogs;
import io.hops.metadata.yarn.entity.PendingEvent;
import io.hops.metadata.yarn.entity.YarnApplicationsToKill;
import io.hops.metadata.yarn.entity.YarnProjectsDailyCost;
import io.hops.metadata.yarn.entity.YarnProjectsQuota;
import io.hops.metadata.yarn.entity.rmstatestore.ApplicationState;
import io.hops.transaction.handler.LightWeightRequestHandler;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import junit.framework.Assert;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.junit.Before;
import org.junit.Test;

public class TestQuotaService {

  private static final Log LOG = LogFactory.getLog(TestQuotaService.class);
  private StorageConnector connector = null;

  @Before
  public void setup() throws IOException {
    Configuration conf = new YarnConfiguration();
    LOG.info("DFS_STORAGE_DRIVER_JAR_FILE : "
            + YarnAPIStorageFactory.DFS_STORAGE_DRIVER_JAR_FILE);
    LOG.info("DFS_STORAGE_DRIVER_JAR_FILE_DEFAULT : "
            + YarnAPIStorageFactory.DFS_STORAGE_DRIVER_JAR_FILE_DEFAULT);
    LOG.info("DFS_STORAGE_DRIVER_CLASS : "
            + YarnAPIStorageFactory.DFS_STORAGE_DRIVER_CLASS);
    LOG.info("DFS_STORAGE_DRIVER_CLASS_DEFAULT : "
            + YarnAPIStorageFactory.DFS_STORAGE_DRIVER_CLASS_DEFAULT);

    YarnAPIStorageFactory.setConfiguration(conf);
    RMStorageFactory.setConfiguration(conf);
    RMUtilities.InitializeDB();
  }

  public void PrepareScenario() throws StorageException, IOException {
    LOG.info("--- START: TestContainerUsage ---");
    LOG.info("--- Checking ContainerStatus ---");

    try {

      final List<RMNode> hopRMNode = new ArrayList<RMNode>();
      hopRMNode.add(new RMNode("Andromeda3:51028"));

      final List<ApplicationState> hopApplicationState
              = new ArrayList<ApplicationState>();
      hopApplicationState.add(new ApplicationState(
              "application_1450009406746_0001", new byte[0], "Project07__rizvi",
              "DistributedShell", "FINISHING", 100000l, 0f, 12.5f));

      final List<ContainersLogs> hopContainersLogs
              = new ArrayList<ContainersLogs>();
      hopContainersLogs.add(new ContainersLogs(
              "container_1450009406746_0001_01_000001", 10, 11,
              ContainerExitStatus.SUCCESS, (float) 2.5));
      hopContainersLogs.add(new ContainersLogs(
              "container_1450009406746_0001_02_000001", 10, 11,
              ContainerExitStatus.ABORTED, (float) 2.5));
      hopContainersLogs.add(new ContainersLogs(
              "container_1450009406746_0001_03_000001", 10, 110,
              ContainerExitStatus.CONTAINER_RUNNING_STATE, (float) 2.5));

      final List<YarnProjectsQuota> hopYarnProjectsQuota
              = new ArrayList<YarnProjectsQuota>();
      hopYarnProjectsQuota.add(new YarnProjectsQuota("Project07", 300, 0));

      LightWeightRequestHandler bomb;
      bomb = new LightWeightRequestHandler(YARNOperationType.TEST) {
        @Override
        public Object performTask() throws IOException {
          connector.beginTransaction();
          connector.writeLock();

          RMNodeDataAccess _rmDA = (RMNodeDataAccess) RMStorageFactory.
                  getDataAccess(RMNodeDataAccess.class);
          _rmDA.addAll(hopRMNode);

          ApplicationStateDataAccess<ApplicationState> _appState
                  = (ApplicationStateDataAccess) RMStorageFactory.getDataAccess(
                          ApplicationStateDataAccess.class);
          _appState.addAll(hopApplicationState);

          ContainersLogsDataAccess<ContainersLogs> _clDA
                  = (ContainersLogsDataAccess) RMStorageFactory.getDataAccess(
                          ContainersLogsDataAccess.class);
          _clDA.addAll(hopContainersLogs);

          YarnProjectsQuotaDataAccess<YarnProjectsQuota> _pqDA
                  = (YarnProjectsQuotaDataAccess) RMStorageFactory.
                  getDataAccess(YarnProjectsQuotaDataAccess.class);
          _pqDA.addAll(hopYarnProjectsQuota);

          connector.commit();
          return null;
        }
      };
      bomb.handle();

    } catch (StorageInitializtionException ex) {
      //LOG.error(ex);
    } catch (StorageException ex) {
      //LOG.error(ex);
    }
  }

  public void CheckProject(float credits, float used) throws IOException {

    Map<String, YarnProjectsQuota> hopYarnProjectsQuotaList;

    LightWeightRequestHandler bomb;
    bomb = new LightWeightRequestHandler(YARNOperationType.TEST) {
      @Override
      public Object performTask() throws IOException {
        connector.beginTransaction();
        connector.writeLock();

        YarnProjectsQuotaDataAccess<YarnProjectsQuota> _pqDA
                = (YarnProjectsQuotaDataAccess) RMStorageFactory.
                getDataAccess(YarnProjectsQuotaDataAccess.class);
        Map<String, YarnProjectsQuota> _hopYarnProjectsQuotaList = _pqDA.
                getAll();

        connector.commit();
        return _hopYarnProjectsQuotaList;
      }
    };
    hopYarnProjectsQuotaList = (Map<String, YarnProjectsQuota>) bomb.handle();

    for (Map.Entry<String, YarnProjectsQuota> _ycl : hopYarnProjectsQuotaList.
            entrySet()) {
      Assert.assertTrue(_ycl.getValue().getProjectid().equalsIgnoreCase(
              "Project07"));
      Assert.assertEquals(credits, _ycl.getValue().getRemainingQuota());
      Assert.assertEquals(used, _ycl.getValue().getTotalUsedQuota());

    }

  }

  public void CheckProjectDailyCost(float used) throws IOException {

    Map<String, YarnProjectsDailyCost> hopYarnProjectsDailyCostList;

    LightWeightRequestHandler bomb;
    bomb = new LightWeightRequestHandler(YARNOperationType.TEST) {
      @Override
      public Object performTask() throws IOException {
        connector.beginTransaction();
        connector.writeLock();

        YarnProjectsDailyCostDataAccess _pdcDA
                = (YarnProjectsDailyCostDataAccess) RMStorageFactory.
                getDataAccess(YarnProjectsDailyCostDataAccess.class);
        Map<String, YarnProjectsDailyCost> hopYarnProjectsDailyCostList
                = _pdcDA.getAll();

        connector.commit();
        return hopYarnProjectsDailyCostList;
      }
    };

    hopYarnProjectsDailyCostList = (Map<String, YarnProjectsDailyCost>) bomb.
            handle();
    long _miliSec = System.currentTimeMillis();
    final long _day = TimeUnit.DAYS.convert(_miliSec, TimeUnit.MILLISECONDS);

    for (Map.Entry<String, YarnProjectsDailyCost> _ypdc
            : hopYarnProjectsDailyCostList.entrySet()) {
      Assert.assertTrue(_ypdc.getValue().getProjectName().equalsIgnoreCase(
              "Project07"));
      Assert.assertTrue(_ypdc.getValue().getProjectUser().equalsIgnoreCase(
              "rizvi"));
      Assert.assertEquals(_day, _ypdc.getValue().getDay());
      Assert.assertEquals(used, _ypdc.getValue().getCreditsUsed());

    }
  }

  @Test //(timeout = 6000)
  public void TestRecover() throws IOException, Exception {

    // Prepare the scenario
    PrepareScenario();

    Configuration conf = new YarnConfiguration();
    //conf.setInt(YarnConfiguration.QUOTAS_TICKS_PER_CREDIT, 10);
    conf.setInt(YarnConfiguration.QUOTAS_MIN_TICKS_CHARGE, 10);

    // Run the Quota Service
    QuotaService qs = new QuotaService();
    qs.init(conf);
    qs.serviceStart();
    Thread.currentThread().sleep(1000);
    qs.serviceStop();

    CheckProject(0, 300);
    CheckProjectDailyCost(300);

  }
  
  @Test
//        (timeout = 6000)
  public void TestStream() throws Exception {
    int initialCredits = 50;
    int totalCost = 0;
    //prepare database
    final List<ApplicationState> hopApplicationState
            = new ArrayList<ApplicationState>();
    hopApplicationState.add(new ApplicationState(
            "application_1450009406746_0001", new byte[0], "Project07__rizvi",
            "DistributedShell", "FINISHING"));
    final List<YarnProjectsQuota> hopYarnProjectsQuota
            = new ArrayList<YarnProjectsQuota>();
    hopYarnProjectsQuota.add(new YarnProjectsQuota("Project07", initialCredits,
            0));

    LightWeightRequestHandler prepareHandler = new LightWeightRequestHandler(
            YARNOperationType.TEST) {
      @Override
      public Object performTask() throws IOException {
        connector.beginTransaction();
        connector.writeLock();

        ApplicationStateDataAccess<ApplicationState> _appState
                = (ApplicationStateDataAccess) RMStorageFactory.getDataAccess(
                        ApplicationStateDataAccess.class);
        _appState.addAll(hopApplicationState);

        YarnProjectsQuotaDataAccess<YarnProjectsQuota> _pqDA
                = (YarnProjectsQuotaDataAccess) RMStorageFactory.
                getDataAccess(YarnProjectsQuotaDataAccess.class);
        _pqDA.addAll(hopYarnProjectsQuota);

        connector.commit();
        return null;
      }
    };
    prepareHandler.handle();

    QuotaService qs = new QuotaService();
    Configuration conf = new YarnConfiguration();
    conf.setInt(YarnConfiguration.QUOTAS_MIN_TICKS_CHARGE, 10);
    qs.init(conf);
    qs.serviceStart();
    //add containers
    for (int i = 0; i < 10; i++) {
      List<ContainersLogs> logs = new ArrayList<ContainersLogs>();

      for (int j = 0; j < i; j++) {
        logs.add(new ContainersLogs("container_1450009406746_0001_0" + i
                + "_00000" + j, i, i,
                ContainerExitStatus.CONTAINER_RUNNING_STATE,(float)0.1));
      }
      qs.insertEvents(logs);
    }
    Thread.sleep(1000);
    //finish some containers
    for (int i = 0; i < 3; i++) {
    List<ContainersLogs> logs = new ArrayList<ContainersLogs>();

      for (int j = 0; j < i; j++) {
        logs.add(new ContainersLogs("container_1450009406746_0001_0" + i
                + "_00000" + j, i, i + 5, ContainerExitStatus.SUCCESS,(float) 0.1));
        totalCost+=1;
      }
    qs.insertEvents(logs);
    }
    Thread.sleep(1000);
    //checkpoint remaining containers
    for (int i = 3; i < 10; i++) {
      List<ContainersLogs> logs = new ArrayList<ContainersLogs>();

      for (int j = 0; j < i; j++) {
        logs.add(new ContainersLogs("container_1450009406746_0001_0" + i
                + "_00000" + j, i, i + 10,
                ContainerExitStatus.CONTAINER_RUNNING_STATE,(float) 0.1));
        totalCost+=1;
      }
      qs.insertEvents(logs);
    }
    Thread.sleep(1000);
    //finish some checkpointed containers
    for (int i = 3; i < 6; i++) {
      List<ContainersLogs> logs = new ArrayList<ContainersLogs>();

      for (int j = 0; j < i; j++) {
        logs.add(new ContainersLogs("container_1450009406746_0001_0" + i
                + "_00000" + j, i, i + 15, ContainerExitStatus.SUCCESS,(float) 0.1));
        totalCost+=1;
      }
      qs.insertEvents(logs);
    }
    Thread.sleep(1000);
    //preempt some containers
    for (int i = 6; i < 9; i++) {
      List<ContainersLogs> logs = new ArrayList<ContainersLogs>();

      for (int j = 0; j < i; j++) {
        logs.add(new ContainersLogs("container_1450009406746_0001_0" + i
                + "_00000" + j, i, i + 16, ContainerExitStatus.PREEMPTED,(float) 0.1));
        totalCost+=1;
      }
      qs.insertEvents(logs);
    }
    Thread.sleep(2000);
    CheckProject(initialCredits - totalCost, totalCost);
    CheckProjectDailyCost(totalCost);
  }

  @Test
  public void TestQuotaServiceChargingProjects() throws Exception {
    int initialCredits = 100;
    int totalCost = 0;
    //prepare database
    final List<ApplicationState> hopApplicationState
            = new ArrayList<ApplicationState>();
    hopApplicationState.add(new ApplicationState(
            "application_1450009406746_0001", new byte[0], "Project07__rizvi",
            "DistributedShell", "FINISHING", 10l, 0f, 12.5f));
    final List<YarnProjectsQuota> hopYarnProjectsQuota
            = new ArrayList<YarnProjectsQuota>();
    hopYarnProjectsQuota.add(new YarnProjectsQuota("Project07", initialCredits,
            0));

    LightWeightRequestHandler prepareHandler = new LightWeightRequestHandler(
            YARNOperationType.TEST) {
              @Override
              public Object performTask() throws IOException {
                connector.beginTransaction();
                connector.writeLock();

                ApplicationStateDataAccess<ApplicationState> _appState
                = (ApplicationStateDataAccess) RMStorageFactory.getDataAccess(
                        ApplicationStateDataAccess.class);
                _appState.addAll(hopApplicationState);

                YarnProjectsQuotaDataAccess<YarnProjectsQuota> _pqDA
                = (YarnProjectsQuotaDataAccess) RMStorageFactory.
                getDataAccess(YarnProjectsQuotaDataAccess.class);
                _pqDA.addAll(hopYarnProjectsQuota);

                connector.commit();
                return null;
              }
            };
    prepareHandler.handle();

    QuotaService qs = new QuotaService();
    Configuration conf = new YarnConfiguration();
    //conf.setInt(YarnConfiguration.QUOTAS_TICKS_PER_CREDIT, 5);
    conf.setInt(YarnConfiguration.QUOTAS_MIN_TICKS_CHARGE, 10);
    qs.init(conf);
    qs.serviceStart();

    // Small test
    List<ContainersLogs> logs = new ArrayList<ContainersLogs>();
    logs.add(new ContainersLogs("container_1450009406746_0001_0" + 1 + "_00000"
            + 1, 10, 10, ContainerExitStatus.CONTAINER_RUNNING_STATE,
            (float) (2.5)));
    logs.add(new ContainersLogs("container_1450009406746_0001_0" + 1 + "_00000"
            + 2, 10, 10, ContainerExitStatus.CONTAINER_RUNNING_STATE,
            (float) (2.5)));
    logs.add(new ContainersLogs("container_1450009406746_0001_0" + 1 + "_00000"
            + 3, 10, 10, ContainerExitStatus.CONTAINER_RUNNING_STATE,
            (float) (2.5)));
    qs.insertEvents(logs);
    Thread.sleep(1000);

    List<ContainersLogs> logs2 = new ArrayList<ContainersLogs>();
    logs2.add(new ContainersLogs("container_1450009406746_0001_0" + 1 + "_00000"
            + 1, 10, 20, ContainerExitStatus.CONTAINER_RUNNING_STATE,
            (float) (2.5)));
    logs2.add(new ContainersLogs("container_1450009406746_0001_0" + 1 + "_00000"
            + 2, 10, 15, ContainerExitStatus.SUCCESS, (float) (2.5)));
    logs2.add(new ContainersLogs("container_1450009406746_0001_0" + 1 + "_00000"
            + 3, 10, 15, ContainerExitStatus.SUCCESS, (float) (2.5)));
    totalCost += (25 + 25 + 25);
    qs.insertEvents(logs2);
    Thread.sleep(1000);

    List<ContainersLogs> logs3 = new ArrayList<ContainersLogs>();
    logs3.add(new ContainersLogs("container_1450009406746_0001_0" + 1 + "_00000"
            + 1, 10, 30, ContainerExitStatus.CONTAINER_RUNNING_STATE,
            (float) (2.5)));
    totalCost += (25);
    qs.insertEvents(logs3);
    Thread.sleep(2000);

    CheckProject(initialCredits - totalCost, totalCost);
    CheckProjectDailyCost(totalCost);
  }

  @Test
  public void TestApplicationKillingWithStreamingSrv() throws Exception {
    Configuration conf = new YarnConfiguration();
    int monitorInterval = 1000;
    conf.set(YarnConfiguration.EVENT_RT_CONFIG_PATH,
            "target/test-classes/RT_EventAPIConfig.ini");
    conf.set(YarnConfiguration.EVENT_SHEDULER_CONFIG_PATH,
            "target/test-classes/RM_EventAPIConfig.ini");
    conf.setInt(YarnConfiguration.QUOTAS_CONTAINERS_LOGS_MONITOR_INTERVAL,
            monitorInterval);
    conf.setInt(YarnConfiguration.QUOTAS_CONTAINERS_LOGS_TICK_INCREMENT, 1);
    conf.
            setBoolean(
                    YarnConfiguration.QUOTAS_CONTAINERS_LOGS_CHECKPOINTS_ENABLED,
                    true);
    conf.setInt(YarnConfiguration.QUOTAS_MIN_TICKS_CHARGE, 10);
    conf.
            setInt(YarnConfiguration.QUOTAS_CONTAINERS_LOGS_CHECKPOINTS_MINTICKS,
                    1);
    conf.setBoolean(YarnConfiguration.DISTRIBUTED_RM, true);
    conf.setBoolean(YarnConfiguration.QUOTAS_ENABLED, true);

        // When RM_HA_ENABLED is disabled the ResourceManager is itself a ResourceTracker.
    // Otherwise the ResourceManager and ResourceTracker are two different nodes.
    conf.setBoolean(YarnConfiguration.RM_HA_ENABLED, true);

    // A new RM will then be a Leader/Schedular
    MockRM rm = new MockRM(conf);
    rm.start();
    // Give some time to start the streaming service.
    Thread.sleep(5000);

    // Check leadership with groupmembership service
    Assert.assertTrue(rm.getRMContext().getGroupMembershipService().isLeader());
    // Check streaming service is started and running
    Assert.assertTrue(RMStorageFactory.isNdbStreaingRunning());

    Assert.assertTrue(rm.getRMContext().isHAEnabled());
    Assert.assertTrue(rm.getRMContext().isDistributedEnabled());
    Assert.assertTrue(rm.getRMContext().isLeadingRT());
    Assert.assertTrue(rm.getRMContext().getGroupMembershipService().isLeader()); // It means this is a Leader/Schedular

    // Start a application 
    RMApp application = rm.submitApp(1 * 1024, "My App", "Riju", null, false,
            null, YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS, null, null, true,
            false, false, null, 5555l, 500.99f, 5.5f);
    Thread.sleep(1000);
    Assert.assertNotNull(application);
    Assert.assertEquals(rm.getRMContext().getRMApps().get(application.
            getApplicationId()).getState(), RMAppState.ACCEPTED);

    LOG.info("RIZ:: submitted application " + application.getApplicationId());
    String app = application.getApplicationId().toString();

    Thread.sleep(2000);
    commitDummyPendingEvent(-1, app);
    Thread.sleep(2000);
    commitDummyPendingEvent(-1, app);
    Thread.sleep(2000);   // This sleep in important for the test to see the streaming functionality      

    Assert.assertEquals(rm.getRMContext().getRMApps().get(application.
            getApplicationId()).getState(), RMAppState.KILLED);
    
    Thread.sleep(10000);
    rm.stop();
  }

  private void commitDummyPendingEvent(final int pendingEvent, final String app) {
    try {
      LightWeightRequestHandler bomb = new LightWeightRequestHandler(
              YARNOperationType.TEST) {
                @Override
                public Object performTask() throws IOException {
                  connector.beginTransaction();
                  connector.writeLock();
                  //int pendingId = pendingEvent;

                  //Insert Pending Event
                  List<PendingEvent> pendingEventsToAdd
                  = new ArrayList<PendingEvent>();
                  pendingEventsToAdd.add(
                          new PendingEvent("killAppNodeId:808", -1, 0, pendingEvent));
                  PendingEventDataAccess pendingEventDA
                  = (PendingEventDataAccess) RMStorageFactory.getDataAccess(
                          PendingEventDataAccess.class);
                  pendingEventDA.addAll(pendingEventsToAdd);
                  
                  final List<YarnApplicationsToKill> applicationListToKill
                  = new ArrayList<YarnApplicationsToKill>();
                  applicationListToKill.add(new YarnApplicationsToKill(
                                  pendingEvent, "killAppNodeId:808", app));
                  YarnApplicationsToKillDataAccess<YarnApplicationsToKill> _appsDA
                  = (YarnApplicationsToKillDataAccess) RMStorageFactory.
                  getDataAccess(YarnApplicationsToKillDataAccess.class);
                  Assert.assertNotNull(_appsDA);
                  _appsDA.addAll(applicationListToKill);
                  
                  
                  
                  connector.commit();
                  return null;
                }
              };
      bomb.handle();
    } catch (IOException ex) {
      LOG.warn("Unable to update container statuses table", ex);
    }
  }
  
  

}
