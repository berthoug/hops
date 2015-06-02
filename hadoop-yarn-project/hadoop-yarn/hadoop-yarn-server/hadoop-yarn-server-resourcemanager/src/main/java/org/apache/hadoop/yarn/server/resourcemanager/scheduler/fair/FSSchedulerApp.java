/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair;

import io.hops.ha.common.FiCaSchedulerAppInfo;
import io.hops.ha.common.TransactionState;
import io.hops.ha.common.TransactionStateImpl;
import io.hops.metadata.yarn.entity.AppSchedulingInfo;
import io.hops.metadata.yarn.entity.fair.PreemptionMap;
import java.io.IOException;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger;
import org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger.AuditConstants;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerFinishedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerImpl;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ActiveUsersManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.util.resource.Resources;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import io.hops.ha.common.FairSchedulerAppInfo;

/**
 * Represents an application attempt from the viewpoint of the Fair Scheduler.
 */
@Private
@Unstable
public class FSSchedulerApp extends SchedulerApplicationAttempt {

  private static final Log LOG = LogFactory.getLog(FSSchedulerApp.class);

  private AppSchedulable appSchedulable;

  final Map<RMContainer, Long> preemptionMap = new HashMap<RMContainer, Long>(); //recovered

  public FSSchedulerApp(ApplicationAttemptId applicationAttemptId,
          String user, FSLeafQueue queue, ActiveUsersManager activeUsersManager,
          RMContext rmContext) {
    super(applicationAttemptId, user, queue, activeUsersManager, rmContext);
  }

  public void recover(AppSchedulingInfo hopInfo, RMStateStore.RMState state) {
    super.recover(hopInfo, state);
    recoverPreemptionMap(getApplicationAttemptId(), state);
  }

  public void setAppSchedulable(AppSchedulable appSchedulable) {
    this.appSchedulable = appSchedulable;
  }

  public AppSchedulable getAppSchedulable() {
    return appSchedulable;
  }

  synchronized public void containerCompleted(RMContainer rmContainer,
      ContainerStatus containerStatus, RMContainerEventType event,
      TransactionState transactionState) {
    
    Container container = rmContainer.getContainer();
    ContainerId containerId = container.getId();

    // Inform the container
    rmContainer.handle(
            new RMContainerFinishedEvent(
                    containerId,
                    containerStatus,
                    event, transactionState)
    );
    LOG.info("Completed container: " + rmContainer.getContainerId()
            + " in state: " + rmContainer.getState() + " event:" + event);

    // Remove from the list of containers
    liveContainers.remove(rmContainer.getContainerId());

    //HOP : remove liveContainers
    ((TransactionStateImpl) transactionState).getSchedulerApplicationInfo().getFiCaSchedulerAppInfo(getApplicationAttemptId()).setLiveContainersToRemove(rmContainer);

    RMAuditLogger.logSuccess(getUser(),
            AuditConstants.RELEASE_CONTAINER, "SchedulerApp",
            getApplicationId(), containerId);

    // Update usage metrics 
    Resource containerResource = rmContainer.getContainer().getResource();
    queue.getMetrics().releaseResources(getUser(), 1, containerResource);
    Resources.subtractFrom(currentConsumption, containerResource);

    //HOP : Update Resources
    ((TransactionStateImpl) transactionState).getSchedulerApplicationInfo().getFiCaSchedulerAppInfo(getApplicationAttemptId()).toUpdateResource(io.hops.metadata.yarn.entity.Resource.CURRENTCONSUMPTION, currentConsumption);

    // remove from preemption map if it is completed
    preemptionMap.remove(rmContainer);
    //HOP : remove preemptive Container
    FairSchedulerAppInfo fair = ((TransactionStateImpl) transactionState).getSchedulerApplicationInfo().getFairSchedulerAppInfo(getApplicationAttemptId());
    fair.removePreemptiveContainer(rmContainer);
  }

  public synchronized void unreserve(FSSchedulerNode node, Priority priority, TransactionState ts) {
    Map<NodeId, RMContainer> reservedContainers = this.reservedContainers.get(priority);
    RMContainer reservedContainer = reservedContainers.remove(node.getNodeID());

    //HOP :: Remove reservedContainers
    FiCaSchedulerAppInfo fica = ((TransactionStateImpl) ts).getSchedulerApplicationInfo().getFiCaSchedulerAppInfo(getApplicationAttemptId());
    fica.removeReservedContainer(reservedContainer);

    if (reservedContainers.isEmpty()) {
      this.reservedContainers.remove(priority);
    }

    // Reset the re-reservation count
    resetReReservations(priority, ts);

    Resource resource = reservedContainer.getContainer().getResource();
    Resources.subtractFrom(currentReservation, resource);
    //HOP : Update Resources
    fica.toUpdateResource(io.hops.metadata.yarn.entity.Resource.CURRENTRESERVATION, currentReservation);

    LOG.info("Application " + getApplicationId() + " unreserved " + " on node "
            + node + ", currently has " + reservedContainers.size() + " at priority "
            + priority + "; currentReservation " + currentReservation);
  }

  public synchronized float getLocalityWaitFactor(
          Priority priority, int clusterNodes) {
    // Estimate: Required unique resources (i.e. hosts + racks)
    int requiredResources
            = Math.max(this.getResourceRequests(priority).size() - 1, 0);

    // waitFactor can't be more than '1' 
    // i.e. no point skipping more than clustersize opportunities
    return Math.min(((float) requiredResources / clusterNodes), 1.0f);
  }

  /**
   * Delay scheduling: We often want to prioritize scheduling of node-local
   * containers over rack-local or off-switch containers. To acheive this we
   * first only allow node-local assigments for a given prioirty level, then
   * relax the locality threshold once we've had a long enough period without
   * succesfully scheduling. We measure both the number of "missed" scheduling
   * opportunities since the last container was scheduled at the current allowed
   * level and the time since the last container was scheduled. Currently we use
   * only the former.
   */
  // Current locality threshold
  final Map<Priority, NodeType> allowedLocalityLevel =
      new HashMap<Priority, NodeType>();

  /**
   * Return the level at which we are allowed to schedule containers, given the
   * current size of the cluster and thresholds indicating how many nodes to
   * fail at (as a fraction of cluster size) before relaxing scheduling
   * constraints.
   */
  public synchronized NodeType getAllowedLocalityLevel(Priority priority,
          int numNodes, double nodeLocalityThreshold, double rackLocalityThreshold, TransactionState ts) {

    FairSchedulerAppInfo fair = ((TransactionStateImpl) ts).getSchedulerApplicationInfo().getFairSchedulerAppInfo(getApplicationAttemptId());

    // upper limit on threshold
    if (nodeLocalityThreshold > 1.0) {
      nodeLocalityThreshold = 1.0;
    }
    if (rackLocalityThreshold > 1.0) {
      rackLocalityThreshold = 1.0;
    }

    // If delay scheduling is not being used, can schedule anywhere
    if (nodeLocalityThreshold < 0.0 || rackLocalityThreshold < 0.0) {
      return NodeType.OFF_SWITCH;
    }

    // Default level is NODE_LOCAL
    if (!allowedLocalityLevel.containsKey(priority)) {
      allowedLocalityLevel.put(priority, NodeType.NODE_LOCAL);
      if (fair != null) {
        fair.addLocalityLevel(priority, NodeType.NODE_LOCAL);
      }

      return NodeType.NODE_LOCAL;
    }

    NodeType allowed = allowedLocalityLevel.get(priority);

    // If level is already most liberal, we're done
    if (allowed.equals(NodeType.OFF_SWITCH)) {
      return NodeType.OFF_SWITCH;
    }

    double threshold =
        allowed.equals(NodeType.NODE_LOCAL) ? nodeLocalityThreshold :
            rackLocalityThreshold;

    // Relax locality constraints once we've surpassed threshold.
    if (getSchedulingOpportunities(priority) > (numNodes * threshold)) {
      if (allowed.equals(NodeType.NODE_LOCAL)) {
        allowedLocalityLevel.put(priority, NodeType.RACK_LOCAL);
        if (fair != null) {
          fair.addLocalityLevel(priority, NodeType.RACK_LOCAL);
        }
        resetSchedulingOpportunities(priority, ts);
      } else if (allowed.equals(NodeType.RACK_LOCAL)) {
        allowedLocalityLevel.put(priority, NodeType.OFF_SWITCH);
        if (fair != null) {
          fair.addLocalityLevel(priority, NodeType.OFF_SWITCH);
        }
        resetSchedulingOpportunities(priority, ts);
      }
    }
    return allowedLocalityLevel.get(priority);
  }

  /**
   * Return the level at which we are allowed to schedule containers. Given the
   * thresholds indicating how much time passed before relaxing scheduling
   * constraints.
   */
  public synchronized NodeType getAllowedLocalityLevelByTime(Priority priority,
          long nodeLocalityDelayMs, long rackLocalityDelayMs,
          long currentTimeMs, TransactionState ts) {

    FairSchedulerAppInfo fair = ((TransactionStateImpl) ts).getSchedulerApplicationInfo().getFairSchedulerAppInfo(getApplicationAttemptId());

    // if not being used, can schedule anywhere
    if (nodeLocalityDelayMs < 0 || rackLocalityDelayMs < 0) {
      return NodeType.OFF_SWITCH;
    }

    // default level is NODE_LOCAL
    if (!allowedLocalityLevel.containsKey(priority)) {
      allowedLocalityLevel.put(priority, NodeType.NODE_LOCAL);
      if (fair != null) {
        fair.addLocalityLevel(priority, NodeType.NODE_LOCAL);
      }
      return NodeType.NODE_LOCAL;
    }

    NodeType allowed = allowedLocalityLevel.get(priority);

    // if level is already most liberal, we're done
    if (allowed.equals(NodeType.OFF_SWITCH)) {
      return NodeType.OFF_SWITCH;
    }

    // check waiting time
    long waitTime = currentTimeMs;
    if (lastScheduledContainer.containsKey(priority)) {
      waitTime -= lastScheduledContainer.get(priority);
    } else {
      waitTime -= appSchedulable.getStartTime();
    }

    long thresholdTime =
        allowed.equals(NodeType.NODE_LOCAL) ? nodeLocalityDelayMs :
            rackLocalityDelayMs;

    if (waitTime > thresholdTime) {
      if (allowed.equals(NodeType.NODE_LOCAL)) {
        allowedLocalityLevel.put(priority, NodeType.RACK_LOCAL);
        if (fair != null) {
          fair.addLocalityLevel(priority, NodeType.NODE_LOCAL);
        }
        resetSchedulingOpportunities(priority, currentTimeMs, ts);
      } else if (allowed.equals(NodeType.RACK_LOCAL)) {
        allowedLocalityLevel.put(priority, NodeType.OFF_SWITCH);
        if (fair != null) {
          fair.addLocalityLevel(priority, NodeType.OFF_SWITCH);
        }
        resetSchedulingOpportunities(priority, currentTimeMs, ts);
      }
    }
    return allowedLocalityLevel.get(priority);
  }

  synchronized public RMContainer allocate(NodeType type, FSSchedulerNode node,
          Priority priority, ResourceRequest request,
          Container container, TransactionState transactionState) {
    // Update allowed locality level
    NodeType allowed = allowedLocalityLevel.get(priority);
    if (allowed != null) {
      if (allowed.equals(NodeType.OFF_SWITCH)
              && (type.equals(NodeType.NODE_LOCAL)
              || type.equals(NodeType.RACK_LOCAL))) {
        this.resetAllowedLocalityLevel(priority, type, transactionState);
      } else if (allowed.equals(NodeType.RACK_LOCAL)
              && type.equals(NodeType.NODE_LOCAL)) {
        this.resetAllowedLocalityLevel(priority, type, transactionState);
      }
    }

    // Required sanity check - AM can call 'allocate' to update resource 
    // request without locking the scheduler, hence we need to check
    if (getTotalRequiredResources(priority) <= 0) {
      return null;
    }

    // Create RMContainer
    RMContainer rmContainer = new RMContainerImpl(container,
            getApplicationAttemptId(), node.getNodeID(),
            appSchedulingInfo.getUser(), rmContext, transactionState);

    // Add it to allContainers list.
    newlyAllocatedContainers.add(rmContainer);
    liveContainers.put(container.getId(), rmContainer);
    LOG.debug("Add liveContainer with container id" + container.getId().toString() + " in transactionstae " + transactionState.toString());

    //HOP : update newlyAllocatedContainers, liveContainers
    FiCaSchedulerAppInfo fica = ((TransactionStateImpl) transactionState).getSchedulerApplicationInfo().getFiCaSchedulerAppInfo(getApplicationAttemptId());
    if (fica != null) {
      fica.setNewlyAllocatedContainersToAdd(rmContainer);
      fica.setLiveContainersToAdd(container.getId(), rmContainer);
    }
    // Update consumption and track allocations
    appSchedulingInfo
        .allocate(type, node, priority, request, container, transactionState);
    Resources.addTo(currentConsumption, container.getResource());

    //HOP : Update Resources
    if (fica != null) {
      fica.toUpdateResource(io.hops.metadata.yarn.entity.Resource.CURRENTCONSUMPTION, currentConsumption);
    }

    // Inform the container
    rmContainer.handle(
            new RMContainerEvent(container.getId(), RMContainerEventType.START, transactionState));

    if (LOG.isDebugEnabled()) {
      LOG.debug("allocate: applicationAttemptId="
              + container.getId().getApplicationAttemptId()
              + " container=" + container.getId() + " host="
              + container.getNodeId().getHost() + " type=" + type);
    }
    RMAuditLogger.logSuccess(getUser(),
            AuditConstants.ALLOC_CONTAINER, "SchedulerApp",
            getApplicationId(), container.getId());

    return rmContainer;
  }

  /**
   * Should be called when the scheduler assigns a container at a higher degree
   * of locality than the current threshold. Reset the allowed locality level to
   * a higher degree of locality.
   */
  public synchronized void resetAllowedLocalityLevel(Priority priority,
          NodeType level, TransactionState ts) {
    NodeType old = allowedLocalityLevel.get(priority);
    LOG.info("Raising locality level from " + old + " to " + level + " at "
            + " priority " + priority);
    allowedLocalityLevel.put(priority, level);

    FairSchedulerAppInfo fair = ((TransactionStateImpl) ts).getSchedulerApplicationInfo().getFairSchedulerAppInfo(getApplicationAttemptId());
    fair.addLocalityLevel(priority, level);
  }

  // related methods
  public void addPreemption(RMContainer container, long time, TransactionState ts) {
    assert preemptionMap.get(container) == null;
    preemptionMap.put(container, time);
    FairSchedulerAppInfo fair = ((TransactionStateImpl) ts).getSchedulerApplicationInfo().getFairSchedulerAppInfo(getApplicationAttemptId());
    fair.addPreemptiveContainer(container, time);
  }

  public Long getContainerPreemptionTime(RMContainer container) {
    return preemptionMap.get(container);
  }

  public Set<RMContainer> getPreemptionContainers() {
    return preemptionMap.keySet();
  }

  @Override
  public FSLeafQueue getQueue() {
    return (FSLeafQueue) super.getQueue();
  }

  private void recoverPreemptionMap(ApplicationAttemptId applicationAttemptId, RMStateStore.RMState state) {
    try {
      List<PreemptionMap> list = state.getPreemptionMaps(applicationAttemptId.toString());
      if (list != null && !list.isEmpty()) {
        for (PreemptionMap hop : list) {
          RMContainer rmContainer = state.getRMContainer(hop.getRmcontainerId(), rmContext);
          preemptionMap.put(rmContainer, hop.getValue());
        }
      }
    } catch (IOException ex) {
      Logger.getLogger(SchedulerApplicationAttempt.class.getName()).log(Level.SEVERE, null, ex);
    }
  }
}
