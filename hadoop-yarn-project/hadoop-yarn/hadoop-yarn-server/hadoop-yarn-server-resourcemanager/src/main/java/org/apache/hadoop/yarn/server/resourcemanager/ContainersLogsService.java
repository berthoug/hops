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
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

public class ContainersLogsService extends CompositeService {

    private static final Log LOG = LogFactory.getLog(ContainersLogsService.class);

    Configuration conf;
    private Thread tickThread;
    private volatile boolean stopped; //Flag for Thread force stop
    private int monitorInterval; //Time in ms till next ContainerStatus read
    private int tickIncrement;
    private boolean checkpointEnabled;
    private int checkpointInterval; //Time in ticks between checkpoints
    private double alertThreshold;
    private double threshold;

    ContainerStatusDataAccess containerStatusDA;
    ContainersLogsDataAccess containersLogsDA;
    YarnVariablesDataAccess yarnVariablesDA;

    Map<String, ContainersLogs> activeContainers
            = new HashMap<String, ContainersLogs>();
    List<ContainersLogs> updateContainers = new ArrayList<ContainersLogs>();
    LinkedBlockingQueue<ContainerStatus> eventContainers
            = new LinkedBlockingQueue<ContainerStatus>();

    YarnVariables tickCounter = new YarnVariables(
            HopYarnAPIUtilities.CONTAINERSTICKCOUNTER,
            0
    );

    // True when service is up to speed with existing statuses and 
    // with events triggered while initializing
    boolean recovered = true;
    public boolean finishedProcessing = false;

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
        // Calculate execution time warning threshold
        this.threshold = this.monitorInterval * alertThreshold;

        // Initialize DataAccesses
        containerStatusDA = (ContainerStatusDataAccess) RMStorageFactory
                .getDataAccess(ContainerStatusDataAccess.class);
        containersLogsDA = (ContainersLogsDataAccess) RMStorageFactory
                .getDataAccess(ContainersLogsDataAccess.class);
        yarnVariablesDA = (YarnVariablesDataAccess) RMStorageFactory
                .getDataAccess(YarnVariablesDataAccess.class);

        // Creates separate thread for retrieving container statuses
        tickThread = new Thread(new TickThread());
        tickThread.setName("ContainersLogs Tick Thread");

        super.serviceInit(conf);
    }

    @Override
    protected void serviceStart() throws Exception {
        LOG.info("Starting containers logs service");

        recover();
        
//        finishedProcessing = false;

//        tickThread.start();

        super.serviceStart();
    }

    @Override
    protected void serviceStop() throws Exception {
        LOG.info("Stopping containers logs service");
        
        activeContainers
            = new HashMap<String, ContainersLogs>();
        updateContainers = new ArrayList<ContainersLogs>();
        eventContainers
            = new LinkedBlockingQueue<ContainerStatus>();
        finishedProcessing = false;

        stopped = true;
        if (tickThread != null) {
            tickThread.interrupt();
        }

        super.serviceStop();
    }

    /**
     * Appends container statuses obtained from events into event queue
     *
     * @param changedContainerStatuses
     */
    public void insertEvent(List<ContainerStatus> changedContainerStatuses) {
        LOG.debug("CL :: New event, size: " + changedContainerStatuses.size());
        for (ContainerStatus cs : changedContainerStatuses) {
            try {
                eventContainers.put(cs);
            } catch (InterruptedException ex) {
                LOG.warn("Unable to insert container status: "
                        + cs.toString() + " inside event queue", ex);
            }
        }
    }

    /**
     * Returns list of latest entries in eventContainers list and removes them
     *
     * @return
     */
    private List<ContainerStatus> getLatestEvents() {
        List<ContainerStatus> oldEvents = new ArrayList<ContainerStatus>();

        while (!eventContainers.isEmpty()) {
            oldEvents.add(eventContainers.poll());
        }

        return oldEvents;
    }

    /**
     * Retrieve tick counter, unfinished containers logs entries and container
     * statuses entries. Merge them and then retrieve events that have arrived
     * since launching service. Merge events and mark recovery as completed.
     */
    public void recover() {
        LOG.info("Starting containers logs recovery");

        try {
            tickCounter = getTickCounter();
            activeContainers = getContainersLogs();

            // Iterate container statuses and update active list
            List<ContainerStatus> containerStatuses
                    = new ArrayList<ContainerStatus>(getContainerStatuses().values());
            recoverContainerStatuses(containerStatuses, tickCounter);

            // Iterate events and update active list
            List<ContainerStatus> latestEvents = getLatestEvents();
            recoverContainerStatuses(latestEvents, tickCounter);

            // Iterate active list, remove COMPLETE from active list
            // and insert all in update list
            for (Iterator<Map.Entry<String, ContainersLogs>> it
                    = activeContainers.entrySet().iterator(); it.hasNext();) {
                ContainersLogs cl = it.next().getValue();

                updateContainers.add(cl);

                if (cl.getExitstatus() != ContainersLogs.CONTAINER_RUNNING_STATE) {
                    it.remove();
                }
            }

            updateContainersLogs(false);

            recovered = true;

            LOG.info("Finished containers logs recovery");
        } catch (Exception ex) {
            LOG.warn("Unable to finish containers logs recovery", ex);
        }
    }

    private void checkEventContainerStatuses(
            List<ContainerStatus> latestEvents
    ) {
        for (ContainerStatus cs : latestEvents) {
            ContainersLogs cl;
            boolean updatable = false;

            if (activeContainers.get(cs.getContainerid()) == null) {
                cl = new ContainersLogs(
                        cs.getContainerid(),
                        tickCounter.getValue(),
                        ContainersLogs.DEFAULT_STOP_TIMESTAMP,
                        ContainersLogs.CONTAINER_RUNNING_STATE
                );

                // Unable to capture start use case,
                if (cs.getState().equals(ContainerState.COMPLETE.toString())) {
                    cl.setExitstatus(ContainersLogs.UNKNOWN_CONTAINER_EXIT);
                }

                activeContainers.put(cl.getContainerid(), cl);
                updatable = true;
            } else {
                cl = activeContainers.get(cs.getContainerid());
            }

            if (cs.getState().equals(ContainerState.COMPLETE.toString())) {
                cl.setStop(tickCounter.getValue());
                cl.setExitstatus(cs.getExitstatus());
                activeContainers.remove(cl.getContainerid());
                updatable = true;
            }

            if (updatable) {
                updateContainers.add(cl);
            }
        }
    }

    /**
     * Go through container status list that is passed(containers statuses or
     * events). Ignore the states which are completed and not known or known,
     * completed and previously were completed.
     *
     * @param recoveryStatuses
     * @param ticks
     */
    private void recoverContainerStatuses(
            List<ContainerStatus> recoveryStatuses,
            YarnVariables ticks
    ) {
        for (ContainerStatus cs : recoveryStatuses) {

            if (activeContainers.get(cs.getContainerid()) == null) {

                // Only care about the ones that are not finished when recovering
                if (!cs.getState().equals(ContainerState.COMPLETE.toString())) {

                    ContainersLogs cl = new ContainersLogs(
                            cs.getContainerid(),
                            ticks.getValue(),
                            ContainersLogs.DEFAULT_STOP_TIMESTAMP,
                            ContainersLogs.CONTAINER_RUNNING_STATE
                    );

                    activeContainers.put(cl.getContainerid(), cl);
                }
            } else {
                ContainersLogs cl = activeContainers.get(cs.getContainerid());

                // If COMPLETE, either transition happened or and old entry
                if (cs.getState().equals(ContainerState.COMPLETE.toString())) {
                    if (cl.getExitstatus() == ContainersLogs.CONTAINER_RUNNING_STATE) {
                        cl.setStop(ticks.getValue());
                        cl.setExitstatus(cs.getExitstatus());
                    } else {
                        activeContainers.remove(cl.getContainerid());
                    }
                }
            }
        }
    }

    /**
     * Updates containers logs table with container status information in update
     * list Also update tick counter in YARN variables table
     */
    private void updateContainersLogs(final boolean updatetTick) {

        try {
            LightWeightRequestHandler containersLogsHandler
                    = new LightWeightRequestHandler(YARNOperationType.TEST) {
                @Override
                public Object performTask() throws StorageException {
                    connector.beginTransaction();
                    connector.writeLock();

                    // Update containers logs table if necessary
                    if (updateContainers.size() > 0) {
                        LOG.debug("CL :: Update containers logs size: " + updateContainers.size());
                        try {
                            containersLogsDA.addAll(updateContainers);
                            updateContainers.clear();
                        } catch (StorageException ex) {
                            LOG.warn("Unable to update containers logs table", ex);
                        }
                    }

                    // Update tick counter
                    if (updatetTick) {
                        yarnVariablesDA.add(tickCounter);
                    }

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
    private Map<String, ContainersLogs> getContainersLogs() {
        Map<String, ContainersLogs> allContainersLogs
                = new HashMap<String, ContainersLogs>();

        try {
            // Retrieve unfinished containers logs entries
            LightWeightRequestHandler allContainersHandler
                    = new LightWeightRequestHandler(YARNOperationType.TEST) {
                @Override
                public Object performTask() throws StorageException {
                    connector.beginTransaction();
                    connector.readCommitted();

                    Map<String, ContainersLogs> allContainersLogs
                            = containersLogsDA.getAll();

                    connector.commit();

                    return allContainersLogs;
                }
            };
            allContainersLogs = (Map<String, ContainersLogs>) allContainersHandler.handle();

        } catch (IOException ex) {
            LOG.warn("Unable to retrieve containers logs table data", ex);
        }

        return allContainersLogs;
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
        for (Map.Entry<String, ContainersLogs> entry : activeContainers.entrySet()) {
            ContainersLogs cl = entry.getValue();

            cl.setStop(tickCounter.getValue());
            updateContainers.add(cl);
        }
    }

    /**
     * Retrieve latest events from the queue;
     * Update active and update lists with latest events
     * Perform checkpoint if necessary
     * Update containers logs table
     */
    public void processTick() {
        long totalTimeStart = System.nanoTime();
        
        try {
        long timeStart = System.nanoTime();
        List<ContainerStatus> latestEvents
                = getLatestEvents();
        LOG.info("{{{EVENT-TIME}}};" + (System.nanoTime() - timeStart) + ";");

        LOG.debug("CL :: Event count: " + latestEvents.size());

        // Go through all events and update active and update lists
        timeStart = System.nanoTime();
        checkEventContainerStatuses(latestEvents);
        LOG.info("{{{ACTIVE-TIME}}};" + (System.nanoTime() - timeStart) + ";");

        // Checkpoint
        if (checkpointEnabled
                && (tickCounter.getValue() % checkpointInterval == 0)) {
            LOG.debug("CL :: Creating checkoint");
            createCheckpoint();
        }

        LOG.debug("CL :: Update list size: " + updateContainers.size());
        LOG.debug("CL :: Active list size: " + activeContainers.size());

        // Update Containers logs table and tick counter
        timeStart = System.nanoTime();
        updateContainersLogs(true);
        LOG.info("{{{WRITE-TIME}}};" + (System.nanoTime() - timeStart) + ";");
        } catch (Exception ex) {
            LOG.warn("Exception in tick processor", ex);
        }
        
        finishedProcessing = true;
        
        LOG.info("{{{TOTAL}}};" + (System.nanoTime() - totalTimeStart) + ";");
    }

    /**
     * Thread that retrieves container statuses, updates active and update
     * lists, and updates containers logs table and tick counter
     */
    private class TickThread implements Runnable {

        @Override
        public void run() {
            while (!stopped && !Thread.currentThread().isInterrupted()) {
                long executionTime = 0;

                try {
                    long startTime = System.currentTimeMillis();

                    if (recovered) {
                        LOG.debug("CL :: Current tick: " + tickCounter.getValue());

                        // Process everything for single tick
                        processTick();

                        // Increment tick counter
                        tickCounter.setValue(tickCounter.getValue() + tickIncrement);
                    } else {
                        LOG.debug("CL :: Not yet recovered");
                    }

                    //Check alert threshold
                    executionTime = System.currentTimeMillis() - startTime;
                    if (threshold < executionTime) {
                        LOG.warn("Monitor interval threshold exceeded!"
                                + " Execution time: "
                                + Long.toString(executionTime) + "ms."
                                + " Threshold: "
                                + Double.toString(threshold) + "ms."
                                + " Consider increasing monitor interval!");
                        //To avoid negative values
                        executionTime = (executionTime > monitorInterval)
                                ? monitorInterval : executionTime;
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
}
