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
package org.apache.hadoop.yarn.server.resourcemanager;

import io.hops.exception.StorageException;
import io.hops.exception.StorageInitializtionException;
import io.hops.metadata.util.HopYarnAPIUtilities;
import io.hops.metadata.util.RMStorageFactory;
import io.hops.metadata.util.RMUtilities;
import io.hops.metadata.util.YarnAPIStorageFactory;
import io.hops.metadata.yarn.dal.ContainerStatusDataAccess;
import io.hops.metadata.yarn.dal.ContainersLogsDataAccess;
import io.hops.metadata.yarn.dal.RMContainerDataAccess;
import io.hops.metadata.yarn.dal.RMNodeDataAccess;
import io.hops.metadata.yarn.dal.YarnVariablesDataAccess;
import io.hops.metadata.yarn.dal.util.YARNOperationType;
import io.hops.metadata.yarn.entity.ContainerStatus;
import io.hops.metadata.yarn.entity.ContainersLogs;
import io.hops.metadata.yarn.entity.RMContainer;
import io.hops.metadata.yarn.entity.RMNode;
import io.hops.metadata.yarn.entity.YarnVariables;
import io.hops.transaction.handler.LightWeightRequestHandler;
import org.apache.hadoop.conf.Configuration;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.Assert;

public class TestContainersLogsService {

    private static final Log LOG = LogFactory.getLog(ContainersLogsService.class);

    private Configuration conf;
    Random random = new Random();

    @Before
    public void setup() throws IOException {
        try {
            conf = new YarnConfiguration();
            YarnAPIStorageFactory.setConfiguration(conf);
            RMStorageFactory.setConfiguration(conf);
            RMUtilities.InitializeDB();
            RMStorageFactory.getConnector().formatStorage();
        } catch (StorageInitializtionException ex) {
            LOG.error(null, ex);
        } catch (IOException ex) {
            LOG.error(null, ex);
        }
    }
    
    @Test
    public void testLoad() throws Exception {
        conf.setBoolean(YarnConfiguration.QUOTAS_CONTAINERS_LOGS_CHECKPOINTS, false);
        
        int numExperiments = 1;
        int containerCount[] = {100};
        int testContainers = 100;
        
        // For each container count
        for (int i = 0; i < containerCount.length; i++) {
            int numContainers = containerCount[i];
            
            // Do N number of experiment repeats
            for (int j = 0; j < numExperiments; j++) {
                long startTime = System.nanoTime();

                LOG.info("{{RUNNING LOAD TEST}};EXPERIMENT;" + j + ";CONTAINERS;" + numContainers);
                
                MockRM rm;
                List<RMNode> rmNodes = generateRMNodesToAdd(1);
                List<RMContainer> rmContainers = 
                        generateRMContainersToAdd(numContainers, 0);
                List<ContainerStatus> containerStatuses
                        = generateContainersStatusToAdd(rmNodes, rmContainers);
                populateDB(rmNodes, rmContainers, containerStatuses);

                rm = new MockRM(conf);
                
                ContainersLogsService cls = rm.getRMContext().getContainersLogsService();
                cls.serviceStart();
                
                List<ContainerStatus> candidates = new ArrayList<ContainerStatus>();
                for(int k = 0; k < testContainers; k++) {
                    candidates.add(containerStatuses.get(k));
                }
                List<ContainerStatus> updatable = 
                        changeContainerStatuses(
                                candidates, 
                                ContainerState.COMPLETE.toString(), 
                                ContainerExitStatus.SUCCESS
                        );
                updateContainerStatuses(updatable);
                Thread.sleep(500);

                List<ContainerStatus> epoch = new ArrayList<ContainerStatus>();
                epoch.add(containerStatuses.get((containerStatuses.size()-1)));
                List<ContainerStatus> updatable2 = 
                        changeContainerStatuses(
                                epoch, 
                                ContainerState.RUNNING.toString(), 
                                ContainerExitStatus.DISKS_FAILED
                        );
                updateContainerStatuses(updatable2);
                Thread.sleep(500);
                
                // Process events
                cls.processTick();
                while (!cls.finishedProcessing) {
                    Thread.sleep(100);
                }
                
                cls.serviceStop();
                
                LOG.info("{{FINISHED LOAD TEST}};EXPERIMENT;" + j + 
                        ";CONTAINERS;" + numContainers + 
                        ";TIME;" + (System.nanoTime() - startTime));
            }
        }
    }


    //Populates DB with fake RM, Containers and status entries
    private void populateDB(
            final List<RMNode> rmNodesToAdd,
            final List<RMContainer> rmContainersToAdd,
            final List<ContainerStatus> containerStatusToAdd
    ) {
        try {
            LightWeightRequestHandler bomb = new LightWeightRequestHandler(
                    YARNOperationType.TEST) {
                @Override
                public Object performTask() throws IOException {
                    connector.beginTransaction();
                    connector.writeLock();

                    // Insert RM nodes
                    RMNodeDataAccess rmNodesDA = (RMNodeDataAccess) RMStorageFactory
                            .getDataAccess(RMNodeDataAccess.class);
                    rmNodesDA.addAll(rmNodesToAdd);

                    // Insert RM Containers
                    RMContainerDataAccess rmcontainerDA
                            = (RMContainerDataAccess) RMStorageFactory
                            .getDataAccess(RMContainerDataAccess.class);
                    rmcontainerDA.addAll(rmContainersToAdd);

                    // Insert container statuses
                    ContainerStatusDataAccess containerStatusDA
                            = (ContainerStatusDataAccess) RMStorageFactory.getDataAccess(
                                    ContainerStatusDataAccess.class);
                    containerStatusDA.addAll(containerStatusToAdd);

                    connector.commit();
                    return null;
                }
            };
            bomb.handle();
        } catch (IOException ex) {
            LOG.warn("Unable to populate DB", ex);
        }
    }

    private void updateContainerStatuses(
            final List<ContainerStatus> containerStatusToAdd) {
        try {
            LightWeightRequestHandler bomb = new LightWeightRequestHandler(
                    YARNOperationType.TEST) {
                @Override
                public Object performTask() throws IOException {
                    connector.beginTransaction();
                    connector.writeLock();

                    // Update container statuses
                    ContainerStatusDataAccess containerStatusDA
                            = (ContainerStatusDataAccess) RMStorageFactory.getDataAccess(
                                    ContainerStatusDataAccess.class);
                    containerStatusDA.addAll(containerStatusToAdd);

                    connector.commit();
                    return null;
                }
            };
            bomb.handle();
        } catch (IOException ex) {
            LOG.warn("Unable to update container statuses table", ex);
        }
    }

    /**
     * Read tick counter from YARN variables table
     *
     * @return
     */
    private YarnVariables getTickCounter() {
        YarnVariables tickCounter = null;

        try {
            LightWeightRequestHandler tickHandler = new LightWeightRequestHandler(
                    YARNOperationType.TEST) {
                @Override
                public Object performTask() throws IOException {
                    YarnVariablesDataAccess yarnVariablesDA
                            = (YarnVariablesDataAccess) RMStorageFactory
                            .getDataAccess(YarnVariablesDataAccess.class);

                    connector.beginTransaction();
                    connector.readCommitted();

                    YarnVariables tc
                            = (YarnVariables) yarnVariablesDA
                            .findById(HopYarnAPIUtilities.CONTAINERSTICKCOUNTER);

                    connector.commit();
                    return tc;
                }
            };
            tickCounter = (YarnVariables) tickHandler.handle();
        } catch (IOException ex) {
            LOG.warn("Unable to retrieve tick counter from YARN variables", ex);
        }

        return tickCounter;
    }

    /**
     * Read all containers logs table entries
     *
     * @return
     */
    private Map<String, ContainersLogs> getContainersLogs() {
        Map<String, ContainersLogs> containersLogs = new HashMap<String, ContainersLogs>();

        try {
            LightWeightRequestHandler allContainersHandler
                    = new LightWeightRequestHandler(YARNOperationType.TEST) {
                @Override
                public Object performTask() throws StorageException {
                    ContainersLogsDataAccess containersLogsDA
                            = (ContainersLogsDataAccess) RMStorageFactory
                            .getDataAccess(ContainersLogsDataAccess.class);
                    connector.beginTransaction();
                    connector.readCommitted();

                    Map<String, ContainersLogs> allContainersLogs
                            = containersLogsDA.getAll();

                    connector.commit();

                    return allContainersLogs;
                }
            };
            containersLogs = (Map<String, ContainersLogs>) allContainersHandler.handle();
        } catch (IOException ex) {
            LOG.warn("Unable to retrieve containers logs", ex);
        }

        return containersLogs;
    }

    private void removeRMNodes(final Collection<RMNode> RMNodes) {
        try {
            LightWeightRequestHandler RMNodesHandler
                    = new LightWeightRequestHandler(YARNOperationType.TEST) {
                @Override
                public Object performTask() throws StorageException {
                    RMNodeDataAccess rmNodesDA
                            = (RMNodeDataAccess) RMStorageFactory
                            .getDataAccess(RMNodeDataAccess.class);

                    connector.beginTransaction();
                    connector.writeLock();

                    rmNodesDA.removeAll(RMNodes);

                    connector.commit();

                    return null;
                }
            };
            RMNodesHandler.handle();
        } catch (IOException ex) {
            LOG.warn("Unable to remove RM nodes from table", ex);
        }
    }

    private List<RMContainer> generateRMContainersToAdd(int nbContainers, int startNo) {
        List<RMContainer> toAdd = new ArrayList<RMContainer>();
        for (int i = startNo; i < (startNo + nbContainers); i++) {
            RMContainer container = new RMContainer("containerid" + i + "_" + random.
                    nextInt(10), "appAttemptId",
                    "nodeId", "user", "reservedNodeId", i, i, i, i, i,
                    "state", "finishedStatusState", i);
            toAdd.add(container);
        }
        return toAdd;
    }

    private List<RMNode> generateRMNodesToAdd(int nbNodes) {
        List<RMNode> toAdd = new ArrayList<RMNode>();
        for (int i = 0; i < nbNodes; i++) {
            RMNode rmNode = new RMNode("nodeid_" + i, "hostName", 1,
                    1, "nodeAddress", "httpAddress", "", 1, "currentState",
                    "version", 1, 1, 0);
            toAdd.add(rmNode);
        }
        return toAdd;
    }

    private List<ContainerStatus> generateContainersStatusToAdd(
            List<RMNode> rmNodesList,
            List<RMContainer> rmContainersList) {
        List<ContainerStatus> toAdd = new ArrayList<ContainerStatus>();
        for (RMContainer rmContainer : rmContainersList) {
            // Get random RM nodes for FK
            int randRMNode = random.nextInt(rmNodesList.size());
            RMNode randomRMNode = rmNodesList.get(randRMNode);

            ContainerStatus status = new ContainerStatus(
                    rmContainer.getContainerIdID(),
                    ContainerState.RUNNING.toString(),
                    null,
                    ContainerExitStatus.SUCCESS,
                    randomRMNode.getNodeId(),
                    0);
            toAdd.add(status);
        }
        return toAdd;
    }

    private List<ContainerStatus> changeContainerStatuses(
            List<ContainerStatus> containerStatuses,
            String newStatus,
            int exitStatus) {
        List<ContainerStatus> toAdd = new ArrayList<ContainerStatus>();

        for (ContainerStatus entry : containerStatuses) {
            ContainerStatus status = new ContainerStatus(
                    entry.getContainerid(),
                    newStatus,
                    null,
                    exitStatus,
                    entry.getRMNodeId(),
                    0);
            toAdd.add(status);
        }
        return toAdd;
    }
}
