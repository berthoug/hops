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
import io.hops.metadata.util.YarnAPIStorageFactory;
import io.hops.metadata.yarn.dal.AppSchedulingInfoDataAccess;
import io.hops.metadata.yarn.dal.ContainerDataAccess;
import io.hops.metadata.yarn.dal.FiCaSchedulerAppNewlyAllocatedContainersDataAccess;
import io.hops.metadata.yarn.dal.RMContainerDataAccess;
import io.hops.metadata.yarn.dal.SchedulerApplicationDataAccess;
import io.hops.metadata.yarn.dal.util.YARNOperationType;
import io.hops.metadata.yarn.entity.AppSchedulingInfo;
import io.hops.metadata.yarn.entity.Container;
import io.hops.metadata.yarn.entity.FiCaSchedulerAppContainer;
import io.hops.metadata.yarn.entity.RMContainer;
import io.hops.metadata.yarn.entity.SchedulerApplication;
import io.hops.transaction.handler.LightWeightRequestHandler;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
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
    for (int i = 0; i < 1000; i++) {
      Container c = new Container("containerId", new byte[78]);
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

    @Test
    public void removeFiCaSchedulerAppNewlyAllocatedContainersClusterJBombing() throws IOException {
                LightWeightRequestHandler addApp = new LightWeightRequestHandler(
                        YARNOperationType.TEST) {
                            @Override
                            public Object performTask() throws IOException {
                                connector.beginTransaction();
                                connector.writeLock();
                                SchedulerApplicationDataAccess appDA =
                                        (SchedulerApplicationDataAccess) RMStorageFactory.getDataAccess(SchedulerApplicationDataAccess.class);
                                List<SchedulerApplication> appToAdd  = new ArrayList<SchedulerApplication>();
                                appToAdd.add(new SchedulerApplication("appid", "user", "queuename"));
                                appDA.addAll(appToAdd);
                                AppSchedulingInfoDataAccess containerDA
                                = (AppSchedulingInfoDataAccess) RMStorageFactory
                                .getDataAccess(AppSchedulingInfoDataAccess.class);
                                List<AppSchedulingInfo> toAdd = new ArrayList<AppSchedulingInfo>();
                                toAdd.add(new AppSchedulingInfo("appid", "appid", "queuename", "user", 0, true, true));
                                containerDA.addAll(toAdd);
                                connector.commit();
                                return null;
                            }
                        };
                addApp.handle();
        
        
        
        
        for (int i = 0; i < 50; i++) {
            final List<FiCaSchedulerAppContainer> toRemove = new ArrayList<FiCaSchedulerAppContainer>();
            for (int j = 0; j < 10; j++) {
                final List<FiCaSchedulerAppContainer> toAdd = new ArrayList<FiCaSchedulerAppContainer>();
                for (int k = 0; k < i*10; k++) {
                    FiCaSchedulerAppContainer c = new FiCaSchedulerAppContainer("appid", "containerid" + j + "_" + k);
                    toAdd.add(c);
                    toRemove.add(c);
                }
                LightWeightRequestHandler filling = new LightWeightRequestHandler(
                        YARNOperationType.TEST) {
                            @Override
                            public Object performTask() throws IOException {
                                connector.beginTransaction();
                                connector.writeLock();
                                FiCaSchedulerAppNewlyAllocatedContainersDataAccess containerDA
                                = (FiCaSchedulerAppNewlyAllocatedContainersDataAccess) RMStorageFactory
                                .getDataAccess(FiCaSchedulerAppNewlyAllocatedContainersDataAccess.class);
                                containerDA.addAll(toAdd);
                                connector.commit();
                                return null;
                            }
                        };
                filling.handle();
            }
            LOG.info("start removing " + toRemove.size());
            long start = System.currentTimeMillis();
            LightWeightRequestHandler bomb = new LightWeightRequestHandler(
                    YARNOperationType.TEST) {
                        @Override
                        public Object performTask() throws IOException {
                            connector.beginTransaction();
                            connector.writeLock();
                            FiCaSchedulerAppNewlyAllocatedContainersDataAccess containerDA
                            = (FiCaSchedulerAppNewlyAllocatedContainersDataAccess) RMStorageFactory
                            .getDataAccess(FiCaSchedulerAppNewlyAllocatedContainersDataAccess.class);
                            containerDA.removeAll(toRemove);
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
