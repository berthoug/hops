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
import io.hops.metadata.util.HopYarnAPIUtilities;
import io.hops.metadata.util.RMStorageFactory;
import io.hops.metadata.yarn.dal.ContainerStatusDataAccess;
import io.hops.metadata.yarn.dal.ContainersLogsDataAccess;
import io.hops.metadata.yarn.dal.YarnVariablesDataAccess;
import io.hops.metadata.yarn.dal.util.YARNOperationType;
import io.hops.metadata.yarn.entity.ContainerStatus;
import io.hops.metadata.yarn.entity.ContainersLogs;
import io.hops.metadata.yarn.entity.YarnVariables;
import io.hops.transaction.handler.LightWeightRequestHandler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

public class ContainersLogsService extends CompositeService {

    private static final Log LOG = LogFactory.getLog(ContainersLogsService.class);

    Configuration conf;
    private Thread checkerThread;
    private volatile boolean stopped; //Flag for Thread force stop
    private int monitorInterval; //Time in ms till next ContainerStatus read
    private int tickIncrement;
    private boolean checkpointEnabled;
    private int checkpointInterval; //Time in ticks between checkpoints
    private double alertThreshold;
    private double threshold;

    Map<String, ActiveContainersLogs> activeContainers;
    List<ContainersLogs> updateContainers;

    ContainerStatusDataAccess containerStatusDA;
    ContainersLogsDataAccess containersLogsDA;
    YarnVariablesDataAccess yarnVariablesDA;

    YarnVariables tickCounter;

    public ContainersLogsService() {
        super(ContainersLogsService.class.getName());
    }

    @Override
    public void serviceInit(Configuration conf) throws Exception {
        LOG.info("Initializing containers logs service");
        this.conf = conf;

        // Initialize config parameters
        this.monitorInterval = this.conf.getInt(
                YarnConfiguration.QUOTAS_CONTAINERS_LOGS_MONITOR_INTERVAL,
                YarnConfiguration.DEFAULT_QUOTAS_CONTAINERS_LOGS_MONITOR_INTERVAL
        );
        this.tickIncrement = this.conf.getInt(
                YarnConfiguration.QUOTAS_CONTAINERS_LOGS_TICK_INCREMENT,
                YarnConfiguration.DEFAULT_QUOTAS_CONTAINERS_LOGS_TICK_INCREMENT
        );
        this.checkpointEnabled = this.conf.getBoolean(
                YarnConfiguration.QUOTAS_CONTAINERS_LOGS_CHECKPOINTS,
                YarnConfiguration.DEFAULT_QUOTAS_CONTAINERS_LOGS_CHECKPOINTS
        );
        this.checkpointInterval = this.conf.getInt(
                YarnConfiguration.QUOTAS_CONTAINERS_LOGS_CHECKPOINTS_TICKS,
                YarnConfiguration.DEFAULT_QUOTAS_CONTAINERS_LOGS_CHECKPOINTS_TICKS
        );
        this.alertThreshold = this.conf.getDouble(
                YarnConfiguration.QUOTAS_CONTAINERS_LOGS_ALERT_THRESHOLD,
                YarnConfiguration.DEFAULT_QUOTAS_CONTAINERS_LOGS_ALERT_THRESHOLD
        );
        this.threshold = this.monitorInterval * alertThreshold;

        // Initialize DataAccesses
        containerStatusDA = (ContainerStatusDataAccess) RMStorageFactory
                .getDataAccess(ContainerStatusDataAccess.class);
        containersLogsDA = (ContainersLogsDataAccess) RMStorageFactory
                .getDataAccess(ContainersLogsDataAccess.class);
        yarnVariablesDA = (YarnVariablesDataAccess) RMStorageFactory
                .getDataAccess(YarnVariablesDataAccess.class);

        // Retrieve unfinished containers logs and tick counter
        tickCounter = getTickCounter();
        activeContainers = getActiveContainersLogs();
        updateContainers = new ArrayList<ContainersLogs>();

        // Creates separate thread for retrieving container statuses
        checkerThread = new Thread(new ContainerStatusChecker());
        checkerThread.setName("ContainersLogs Container Status Checker");

        super.serviceInit(conf);
    }

    @Override
    protected void serviceStart() throws Exception {
        LOG.info("Starting containers logs service");

        checkerThread.start();

        super.serviceStart();
    }

    @Override
    protected void serviceStop() throws Exception {
        LOG.info("Stopping containers logs service");

        stopped = true;
        if (checkerThread != null) {
            checkerThread.interrupt();
        }

        super.serviceStop();
    }

    /**
     * Loop through retrieved container statuses, and check if it exists in
     * active list Also check if container is COMPLETED, populate update list if
     * necessary
     *
     * @param containerStatuses
     */
    private void checkContainerStatuses(Map<String, ContainerStatus> containerStatuses) {

        for (Map.Entry<String, ContainerStatus> entry : containerStatuses.entrySet()) {
            boolean updatable = false; //Indicates wether container will be updated in DB
            ContainerStatus cs = entry.getValue();
            ActiveContainersLogs acl;

            // Check if container exists in active list, if not add, if yes retrieve
            if (activeContainers.get(entry.getKey()) == null) {
                acl = new ActiveContainersLogs(
                        cs.getContainerid(),
                        tickCounter.getValue(),
                        ContainersLogs.DEFAULT_STOP_TIMESTAMP,
                        ContainersLogs.CONTAINER_RUNNING_STATE
                );

                activeContainers.put(entry.getKey(), acl);
                updatable = true;
            } else {
                acl = activeContainers.get(entry.getKey());
            }
            acl.setFound(true);

            // If state COMPLETE and not yet been completed mark stop time & exit state
            if (!acl.getCompleted() && cs.getState().equals(ContainerState.COMPLETE.toString())) {
                acl.setStop(tickCounter.getValue());
                acl.setExitstatus(cs.getExitstatus());
                acl.setCompleted(true);
                updatable = true;
            }

            // Check if container needs to be added/updated in DB
            if (updatable) {
                updateContainers.add(acl.getContainersLogs());
            }
        }
    }

    /**
     * Loop through active list and remove completed containers or check if a
     * container completed state was missed
     */
    private void checkActiveContainerStatuses() {
        for (Iterator<Map.Entry<String, ActiveContainersLogs>> it
                = activeContainers.entrySet().iterator(); it.hasNext();) {
            ActiveContainersLogs acl = it.next().getValue();

            // If container status has not been seen its either completed, or we were unable to capture it
            if (!acl.getFound()) {
                if (!acl.getCompleted()) {
                    //If status not found and not completed, unable to capture containers status
                    acl.setStop(tickCounter.getValue());
                    acl.setExitstatus(ContainersLogs.UNKNOWN_CONTAINER_EXIT);
                    updateContainers.add(acl.getContainersLogs());
                }

                it.remove();
            }
            acl.setFound(false);
        }
    }

    /**
     * Updates containers logs table with container status information in update
     * list Also update tick counter in YARN variables table
     */
    private void updateContainersLogs() {

        try {
            LightWeightRequestHandler containersLogsHandler
                    = new LightWeightRequestHandler(YARNOperationType.TEST) {
                @Override
                public Object performTask() throws StorageException {
                    connector.beginTransaction();
                    connector.writeLock();

                    // Update containers logs table if necessary
                    if (updateContainers.size() > 0) {
                        LOG.debug("Update containers logs size: " + updateContainers.size());
                        try {
                            containersLogsDA.addAll(updateContainers);
                            updateContainers.clear();
                        } catch (StorageException ex) {
                            LOG.warn("Unable to update containers logs table", ex);
                        }
                    }

                    // Update tick counter
                    yarnVariablesDA.add(tickCounter);

                    connector.commit();
                    return null;
                }
            };
            containersLogsHandler.handle();
        } catch (IOException ex) {
            LOG.warn("Unable to update containers logs and tick counter", ex);
        }
    }

    /**
     * Retrieves unfinished containers logs entries Used when initializing
     * active list
     *
     * @return
     */
    private Map<String, ActiveContainersLogs> getActiveContainersLogs() {
        Map<String, ActiveContainersLogs> activeList
                = new HashMap<String, ActiveContainersLogs>();

        try {
            Map<String, ContainersLogs> allContainersLogs;

            // Retrieve unfinished containers logs entries
            LightWeightRequestHandler allContainersHandler
                    = new LightWeightRequestHandler(YARNOperationType.TEST) {
                @Override
                public Object performTask() throws StorageException {
                    connector.beginTransaction();
                    connector.readCommitted();

                    Map<String, ContainersLogs> allContainersLogs
                            = containersLogsDA.getByExitStatus(
                                    ContainersLogs.CONTAINER_RUNNING_STATE
                            );

                    connector.commit();

                    return allContainersLogs;
                }
            };
            allContainersLogs = (Map<String, ContainersLogs>) allContainersHandler.handle();

            // Store table entries into active list
            for (Map.Entry<String, ContainersLogs> entry : allContainersLogs.entrySet()) {
                ContainersLogs cl = entry.getValue();
                ActiveContainersLogs acl;

                acl = new ActiveContainersLogs(
                        cl.getContainerid(),
                        cl.getStart(),
                        cl.getStop(),
                        cl.getExitstatus()
                );

                activeList.put(entry.getKey(), acl);
            }
        } catch (IOException ex) {
            LOG.warn("Unable to retrieve containers logs table data", ex);
        }

        return activeList;
    }

    /**
     * Retrieves containers logs tick counter from YARN variables
     *
     * @return
     */
    private YarnVariables getTickCounter() {
        YarnVariables tc = new YarnVariables(
                HopYarnAPIUtilities.CONTAINERSTICKCOUNTER,
                0
        );

        try {
            YarnVariables found;

            LightWeightRequestHandler tickCounterHandler
                    = new LightWeightRequestHandler(YARNOperationType.TEST) {
                @Override
                public Object performTask() throws StorageException {
                    connector.beginTransaction();
                    connector.readCommitted();

                    YarnVariables tickCounterVariable
                            = (YarnVariables) yarnVariablesDA
                            .findById(HopYarnAPIUtilities.CONTAINERSTICKCOUNTER);

                    connector.commit();

                    return tickCounterVariable;
                }
            };
            found = (YarnVariables) tickCounterHandler.handle();

            if (found != null) {
                tc = found;
            }
        } catch (IOException ex) {
            LOG.warn("Unable to retrieve tick counter from YARN variables", ex);
        }

        return tc;
    }

    /**
     * Retrieves all container statuses from `yarn_containerstatus` table
     *
     * @return
     */
    private Map<String, ContainerStatus> getContainerStatuses() {
        Map<String, ContainerStatus> allContainerStatuses
                = new HashMap<String, ContainerStatus>();

        try {
            LightWeightRequestHandler containerStatusHandler
                    = new LightWeightRequestHandler(YARNOperationType.TEST) {
                @Override
                public Object performTask() throws StorageException {
                    connector.beginTransaction();
                    connector.readCommitted();

                    Map<String, ContainerStatus> containerStatuses
                            = containerStatusDA.getAll();

                    connector.commit();

                    return containerStatuses;
                }
            };
            allContainerStatuses
                    = (Map<String, ContainerStatus>) containerStatusHandler.handle();
        } catch (IOException ex) {
            LOG.warn("Unable to retrieve container statuses", ex);
        }
        return allContainerStatuses;
    }

    /**
     * Loop active list and add all found & not completed container statuses to
     * update list. This ensures that whole running time is not lost.
     */
    private void createCheckpoint() {
        for (Map.Entry<String, ActiveContainersLogs> entry : activeContainers.entrySet()) {
            ActiveContainersLogs acl = entry.getValue();

            if (!acl.getCompleted()) {
                acl.setStop(tickCounter.getValue());
                updateContainers.add(acl.getContainersLogs());
            }
        }
    }

    /**
     * Thread that retrieves container statuses, updates active and update
     * lists, and updates containers logs table and tick counter
     */
    private class ContainerStatusChecker implements Runnable {

        @Override
        public void run() {
            while (!stopped && !Thread.currentThread().isInterrupted()) {
                long executionTime = 0;

                try {
                    long startTime = System.currentTimeMillis();

                    LOG.debug("Current tick: " + tickCounter.getValue());

                    Map<String, ContainerStatus> allContainerStatuses
                            = getContainerStatuses();

                    LOG.debug("Retrieved container status count: " + allContainerStatuses.size());

                    // Go through all containers statuses and update active and update lists
                    checkContainerStatuses(allContainerStatuses);

                    // Go through all active container statuses
                    checkActiveContainerStatuses();

                    // Checkpoint
                    if (checkpointEnabled
                            && (tickCounter.getValue() % checkpointInterval == 0)) {
                        LOG.debug("Creating checkoint");
                        createCheckpoint();
                    }

                    LOG.debug("Update list size: " + updateContainers.size());
                    LOG.debug("Active list size: " + activeContainers.size());

                    // Update Containers logs table and tick counter
                    updateContainersLogs();

                    // Increment tick counter
                    tickCounter.setValue(tickCounter.getValue() + tickIncrement);

                    //Check alert threshold
                    executionTime = System.currentTimeMillis() - startTime;
                    if (threshold < executionTime) {
                        LOG.debug("Monitor interval threshold exceeded!"
                                + " Execution time: "
                                + Long.toString(executionTime) + "ms."
                                + " Threshold: "
                                + Double.toString(threshold) + "ms."
                                + " Consider increasing monitor interval!");
                    }
                } catch (Exception ex) {
                    LOG.warn("Exception in containers logs thread loop", ex);
                }

                try {
                    Thread.sleep(monitorInterval - executionTime);
                } catch (InterruptedException ex) {
                    LOG.info(getName() + " thread interrupted", ex);
                    break;
                }
            }
        }
    }

    /**
     * Extends ContainersLogs adding two more flags
     *
     * @param found, represents a flag in containers status has been retrieved
     * @param completed, represents a flag if container has been completed
     */
    private class ActiveContainersLogs extends ContainersLogs {

        private boolean found = false;
        private boolean completed = false;

        public ActiveContainersLogs(
                String containerid,
                int start,
                int stop,
                int exitstatus
        ) {
            super(containerid, start, stop, exitstatus);
        }

        public void setFound(boolean found) {
            this.found = found;
        }

        public boolean getFound() {
            return this.found;
        }

        public void setCompleted(boolean completed) {
            this.completed = completed;
        }

        public boolean getCompleted() {
            return this.completed;
        }

        public ContainersLogs getContainersLogs() {
            return new ContainersLogs(
                    this.getContainerid(),
                    this.getStart(),
                    this.getStop(),
                    this.getExitstatus()
            );
        }
    }
}
