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
import io.hops.metadata.util.RMStorageFactory;
import io.hops.metadata.yarn.dal.ContainerIdToCleanDataAccess;
import io.hops.metadata.yarn.dal.ContainerStatusDataAccess;
import io.hops.metadata.yarn.dal.FinishedApplicationsDataAccess;
import io.hops.metadata.yarn.dal.JustLaunchedContainersDataAccess;
import io.hops.metadata.yarn.dal.NextHeartbeatDataAccess;
import io.hops.metadata.yarn.dal.NodeHBResponseDataAccess;
import io.hops.metadata.yarn.dal.PendingEventDataAccess;
import io.hops.metadata.yarn.dal.UpdatedContainerInfoDataAccess;
import io.hops.metadata.yarn.entity.ContainerId;
import io.hops.metadata.yarn.entity.ContainerStatus;
import io.hops.metadata.yarn.entity.FinishedApplications;
import io.hops.metadata.yarn.entity.JustLaunchedContainers;
import io.hops.metadata.yarn.entity.NextHeartbeat;
import io.hops.metadata.yarn.entity.NodeHBResponse;
import io.hops.metadata.yarn.entity.PendingEvent;
import io.hops.metadata.yarn.entity.UpdatedContainerInfo;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class RMNodeInfoAgregate {

    public static final Log LOG = LogFactory.getLog(RMNodeInfoAgregate.class);
    List<JustLaunchedContainers> toAddJustLaunchedContainers
            = new ArrayList<JustLaunchedContainers>();
    List<ContainerStatus> toAddContainerStatus = new ArrayList<ContainerStatus>();
    List<JustLaunchedContainers> toRemoveJustLaunchedContainers
            = new ArrayList<JustLaunchedContainers>();
    ArrayList<ContainerId> toAddContainerIdToClean = new ArrayList<ContainerId>();
    ArrayList<ContainerId> toRemoveContainerIdToClean
            = new ArrayList<ContainerId>();
    Set<FinishedApplications> toAddFinishedApplications
            = new HashSet<FinishedApplications>();
    Set<PendingEvent> toAddPendingEvents
            = new HashSet<PendingEvent>();
    Set<PendingEvent> toRemovePendingEvents
            = new HashSet<PendingEvent>();
    Set<FinishedApplications> toRemoveFinishedApplications
            = new HashSet<FinishedApplications>();
    ArrayList<UpdatedContainerInfo> uciToAdd
            = new ArrayList<UpdatedContainerInfo>();
    Set<UpdatedContainerInfo> uciToRemove = new HashSet<UpdatedContainerInfo>();
    List<NodeHBResponse> hbResponseToAdd = new ArrayList<NodeHBResponse>();
    List<NextHeartbeat> nextHeartBeatToUpdate = new ArrayList<NextHeartbeat>();

    public void addAllContainersStatusToAdd(
            List<ContainerStatus> toAddContainerStatus) {
        this.toAddContainerStatus.addAll(toAddContainerStatus);
    }

    public void addAllJustLaunchedContainersToAdd(
            List<JustLaunchedContainers> toAddJustLaunchedContainers) {
        this.toAddJustLaunchedContainers.addAll(toAddJustLaunchedContainers);
    }

    public void addAllPendingEventsToAdd(ArrayList<PendingEvent> toAddPendingEvents) {
        this.toAddPendingEvents.addAll(toAddPendingEvents);
    }

    public void addAllPendingEventsToRemove(ArrayList<PendingEvent> toRemovePendingEvents) {
        this.toRemovePendingEvents.addAll(toRemovePendingEvents);
    }

    public void addAllJustLaunchedContainersToRemove(
            List<JustLaunchedContainers> toRemoveJustLaunchedContainers) {
        this.toRemoveJustLaunchedContainers.addAll(toAddJustLaunchedContainers);
    }

    public void addAllContainersToCleanToAdd(
            ArrayList<ContainerId> toAddContainerIdToClean) {
        this.toAddContainerIdToClean.addAll(toAddContainerIdToClean);
    }

    public void addAllContainerToCleanToRemove(
            ArrayList<ContainerId> toRemoveContainerIdToClean) {
        this.toRemoveContainerIdToClean.addAll(toRemoveContainerIdToClean);
    }

    public void addAllFinishedAppToAdd(
            ArrayList<FinishedApplications> toAddFinishedApplications) {
        this.toAddFinishedApplications.addAll(toAddFinishedApplications);
    }

    public void addAllFinishedAppToRemove(
            ArrayList<FinishedApplications> toRemoveFinishedApplications) {
        this.toRemoveFinishedApplications.addAll(toAddFinishedApplications);
    }

    public void addAllUpdatedContainerInfoToAdd(
            ArrayList<UpdatedContainerInfo> uciToAdd) {
        this.uciToAdd.addAll(uciToAdd);
    }

    public void addAllUpdatedContainerInfoToRemove(
            Set<UpdatedContainerInfo> uciToRemove) {
        this.uciToRemove.addAll(uciToRemove);
    }

    public void addLastHeartbeatResponse(NodeHBResponse toAdd) {
        hbResponseToAdd.add(toAdd);
    }

    public void addNextHeartbeat(NextHeartbeat nextHeartbeat) {
        nextHeartBeatToUpdate.add(nextHeartbeat);
    }

    static double totalt1 = 0;
    static double totalt2 = 0;
    static double totalt3 = 0;
    static double totalt4 = 0;
    static double totalt5 = 0;
    static double totalt6 = 0;
    static double totalt7 = 0;
    static double totalt8 = 0;
    static double totalt9 = 0;
    static double totalt10 = 0;
    static double totalt11 = 0;
    static double totalt12 = 0;
    static double totalt13 = 0;
    static long nbFinish = 0;

    public void persist(NodeHBResponseDataAccess hbDA,
            ContainerIdToCleanDataAccess cidToCleanDA,
            JustLaunchedContainersDataAccess justLaunchedContainersDA,
            UpdatedContainerInfoDataAccess updatedContainerInfoDA,
            FinishedApplicationsDataAccess faDA, ContainerStatusDataAccess csDA, PendingEventDataAccess persistedEventsDA)
            throws StorageException {
        Long start = System.currentTimeMillis();
        persistContainerStatusToAdd(csDA);
        totalt1 = totalt1 + System.currentTimeMillis() - start;
        persistJustLaunchedContainersToAdd(justLaunchedContainersDA);
        totalt2 = totalt2 + System.currentTimeMillis() - start;
        persistJustLaunchedContainersToRemove(justLaunchedContainersDA);
        totalt3 = totalt3 + System.currentTimeMillis() - start;
        persistContainerToCleanToAdd(cidToCleanDA);
        totalt4 = totalt4 + System.currentTimeMillis() - start;
        persistContainerToCleanToRemove(cidToCleanDA);
        totalt5 = totalt5 + System.currentTimeMillis() - start;
        persistFinishedApplicationToAdd(faDA);
        totalt6 = totalt6 + System.currentTimeMillis() - start;
        persistFinishedApplicationToRemove(faDA);
        totalt7 = totalt7 + System.currentTimeMillis() - start;
        persistNodeUpdateQueueToAdd(updatedContainerInfoDA);
        totalt8 = totalt8 + System.currentTimeMillis() - start;
        persistNodeUpdateQueueToRemove(updatedContainerInfoDA);
        totalt9 = totalt9 + System.currentTimeMillis() - start;
        persistLatestHeartBeatResponseToAdd(hbDA);
        totalt10 = totalt10 + System.currentTimeMillis() - start;
        persistNextHeartbeat();
        totalt11 = totalt11 + System.currentTimeMillis() - start;
        persistPendingEventsToAdd(persistedEventsDA);
        totalt12 = totalt12 + System.currentTimeMillis() - start;
        persistPendingEventsToRemove(persistedEventsDA);
        totalt13 = totalt13 + System.currentTimeMillis() - start;
        nbFinish++;
        if (nbFinish % 100 == 0) {
            double avgt1 = totalt1 / nbFinish;
            double avgt2 = totalt2 / nbFinish;
            double avgt3 = totalt3 / nbFinish;
            double avgt4 = totalt4 / nbFinish;
            double avgt5 = totalt5 / nbFinish;
            double avgt6 = totalt6 / nbFinish;
            double avgt7 = totalt7 / nbFinish;
            double avgt8 = totalt8 / nbFinish;
            double avgt9 = totalt9 / nbFinish;
            double avgt10 = totalt10 / nbFinish;
            double avgt11 = totalt11 / nbFinish;
            double avgt12 = totalt12 / nbFinish;
            double avgt13 = totalt13 / nbFinish;
            LOG.debug("avg time commit node info agregate: " + avgt1 + ", " + avgt2
                    + ", " + avgt3 + ", " + avgt4 + ", " + avgt5 + ", " + avgt6 + ", "
                    + avgt7 + ", " + avgt8 + ", " + avgt9 + ", " + avgt10 + ", "
                    + avgt11 + ", "
                    + avgt12 + ", "
                    + avgt13);
        }
    }

    private void persistContainerStatusToAdd(ContainerStatusDataAccess csDA)
            throws StorageException {
        csDA.addAll(toAddContainerStatus);
    }

    public void persistJustLaunchedContainersToAdd(
            JustLaunchedContainersDataAccess justLaunchedContainersDA) throws
            StorageException {
        justLaunchedContainersDA.addAll(toAddJustLaunchedContainers);
    }

    public void persistJustLaunchedContainersToRemove(
            JustLaunchedContainersDataAccess justLaunchedContainersDA)
            throws StorageException {
        justLaunchedContainersDA.removeAll(toRemoveJustLaunchedContainers);
    }

    public void persistContainerToCleanToAdd(
            ContainerIdToCleanDataAccess cidToCleanDA) throws StorageException {
        cidToCleanDA.addAll(toAddContainerIdToClean);
    }

    public void persistContainerToCleanToRemove(
            ContainerIdToCleanDataAccess cidToCleanDA) throws StorageException {
        cidToCleanDA.removeAll(toRemoveContainerIdToClean);
    }

    public void persistFinishedApplicationToAdd(
            FinishedApplicationsDataAccess faDA) throws StorageException {
        faDA.addAll(toAddFinishedApplications);
    }

    public void persistFinishedApplicationToRemove(
            FinishedApplicationsDataAccess faDA) throws StorageException {
        faDA.removeAll(toRemoveFinishedApplications);
    }

    public void persistNodeUpdateQueueToAdd(
            UpdatedContainerInfoDataAccess updatedContainerInfoDA) throws
            StorageException {
        updatedContainerInfoDA.addAll(uciToAdd);
    }

    public void persistNodeUpdateQueueToRemove(
            UpdatedContainerInfoDataAccess updatedContainerInfoDA) throws
            StorageException {
        updatedContainerInfoDA.removeAll(uciToRemove);
    }

    public void persistPendingEventsToAdd(
            PendingEventDataAccess persistedEventsDA) throws
            StorageException {
        persistedEventsDA.addAll(toAddPendingEvents);
    }

    public void persistPendingEventsToRemove(
            PendingEventDataAccess persistedEventsDA) throws
            StorageException {
        persistedEventsDA.removeAll(toRemovePendingEvents);
    }

    public void persistLatestHeartBeatResponseToAdd(NodeHBResponseDataAccess hbDA)
            throws StorageException {
        hbDA.addAll(hbResponseToAdd);
    }

    public void persistNextHeartbeat() throws StorageException {
        NextHeartbeatDataAccess nextHeartbeatDA
                = (NextHeartbeatDataAccess) RMStorageFactory
                .getDataAccess(NextHeartbeatDataAccess.class);
        nextHeartbeatDA.updateAll(nextHeartBeatToUpdate);
    }
}
