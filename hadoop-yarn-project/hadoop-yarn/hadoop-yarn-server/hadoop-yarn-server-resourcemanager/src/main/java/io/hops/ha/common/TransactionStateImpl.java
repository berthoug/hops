/*
 * Copyright (C) 2015 hops.io.
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

import io.hops.StorageConnector;
import io.hops.exception.StorageException;
import io.hops.metadata.util.RMStorageFactory;
import io.hops.metadata.util.RMUtilities;
import io.hops.metadata.yarn.dal.ContainerDataAccess;
import io.hops.metadata.yarn.dal.ContainerIdToCleanDataAccess;
import io.hops.metadata.yarn.dal.ContainerStatusDataAccess;
import io.hops.metadata.yarn.dal.FiCaSchedulerNodeDataAccess;
import io.hops.metadata.yarn.dal.FinishedApplicationsDataAccess;
import io.hops.metadata.yarn.dal.JustLaunchedContainersDataAccess;
import io.hops.metadata.yarn.dal.LaunchedContainersDataAccess;
import io.hops.metadata.yarn.dal.NodeDataAccess;
import io.hops.metadata.yarn.dal.NodeHBResponseDataAccess;
import io.hops.metadata.yarn.dal.PendingEventDataAccess;
import io.hops.metadata.yarn.dal.QueueMetricsDataAccess;
import io.hops.metadata.yarn.dal.RMContainerDataAccess;
import io.hops.metadata.yarn.dal.RMContextInactiveNodesDataAccess;
import io.hops.metadata.yarn.dal.RMNodeDataAccess;
import io.hops.metadata.yarn.dal.ResourceDataAccess;
import io.hops.metadata.yarn.dal.UpdatedContainerInfoDataAccess;
import io.hops.metadata.yarn.dal.capacity.CSLeafQueueUserInfoDataAccess;
import io.hops.metadata.yarn.dal.capacity.CSQueueDataAccess;
import io.hops.metadata.yarn.dal.fair.FSSchedulerNodeDataAccess;
import io.hops.metadata.yarn.dal.rmstatestore.AllocateResponseDataAccess;
import io.hops.metadata.yarn.dal.rmstatestore.AllocatedContainersDataAccess;
import io.hops.metadata.yarn.dal.rmstatestore.ApplicationAttemptStateDataAccess;
import io.hops.metadata.yarn.dal.rmstatestore.ApplicationStateDataAccess;
import io.hops.metadata.yarn.dal.rmstatestore.RanNodeDataAccess;
import io.hops.metadata.yarn.dal.rmstatestore.UpdatedNodeDataAccess;
import io.hops.metadata.yarn.entity.FiCaSchedulerNode;
import io.hops.metadata.yarn.entity.FiCaSchedulerNodeInfos;
import io.hops.metadata.yarn.entity.LaunchedContainers;
import io.hops.metadata.yarn.entity.PendingEvent;
import io.hops.metadata.yarn.entity.RMContainer;
import io.hops.metadata.yarn.entity.RMNode;
import io.hops.metadata.yarn.entity.Resource;
import io.hops.metadata.yarn.entity.rmstatestore.AllocateResponse;
import io.hops.metadata.yarn.entity.rmstatestore.ApplicationAttemptState;
import io.hops.metadata.yarn.entity.rmstatestore.ApplicationState;
import io.hops.metadata.yarn.entity.rmstatestore.UpdatedNode;
import io.hops.metadata.yarn.entity.rmstatestore.RanNode;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.AllocateResponsePBImpl;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.server.resourcemanager.ApplicationMasterService.AllocateResponseLock;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.impl.pb.ApplicationAttemptStateDataPBImpl;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.impl.pb.ApplicationStateDataPBImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeImpl;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerPBImpl;

import static org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.LOG;

public class TransactionStateImpl extends TransactionState {

  //Type of TransactionImpl to know which finishRPC to call
  //In future implementation this will be removed as a single finishRPC will exist
  private final TransactionType type;
  //NODE
  private Map<String, RMNode>
      rmNodesToUpdate = new ConcurrentHashMap<String, RMNode>();
  private final Map<NodeId, RMNodeInfo> rmNodeInfos =
      new ConcurrentSkipListMap<NodeId, RMNodeInfo>();
  private final Map<String, FiCaSchedulerNodeInfoToUpdate>
      ficaSchedulerNodeInfoToUpdate =
      new HashMap<String, FiCaSchedulerNodeInfoToUpdate>();
  private final Map<String, FiCaSchedulerNodeInfos>
      ficaSchedulerNodeInfoToAdd =
      new HashMap<String, FiCaSchedulerNodeInfos>();
  private final Map<String, FiCaSchedulerNodeInfos>
      ficaSchedulerNodeInfoToRemove =
      new HashMap<String, FiCaSchedulerNodeInfos>();
  private final FairSchedulerNodeInfo fairschedulerNodeInfo =
      new FairSchedulerNodeInfo();
  private final HashMap<String, RMContainer> rmContainersToUpdate =
      new HashMap<String, RMContainer>();
  private final List<io.hops.metadata.yarn.entity.Container> toAddContainers =
          new ArrayList<io.hops.metadata.yarn.entity.Container>();
  private final CSQueueInfo csQueueInfo = new CSQueueInfo();
  
  //APP
  private final SchedulerApplicationInfo schedulerApplicationInfo;
  private final Map<ApplicationId, ApplicationState> applicationsToAdd = new HashMap<ApplicationId, ApplicationState>();
  private final Map<ApplicationId, List<UpdatedNode>> updatedNodeIdToAdd = new HashMap<ApplicationId, List<UpdatedNode>>();
  private final List<ApplicationId> applicationsStateToRemove =
      new ArrayList<ApplicationId>();
  private final HashMap<String, ApplicationAttemptState> appAttempts =
      new HashMap<String, ApplicationAttemptState>();
  private final HashMap<ApplicationAttemptId, Map<Integer, RanNode>>ranNodeToAdd =
          new HashMap<ApplicationAttemptId, Map<Integer, RanNode>>();
  private final HashMap<ApplicationAttemptId, AllocateResponse>
      allocateResponsesToAdd =
      new HashMap<ApplicationAttemptId, AllocateResponse>();
  private final Set<AllocateResponse> allocateResponsesToRemove =
      new HashSet<AllocateResponse>();
  
  
  //COMTEXT
  private final RMContextInfo rmcontextInfo = new RMContextInfo();
  private org.apache.hadoop.yarn.api.records.Resource clusterResourceToUpdate;
  private org.apache.hadoop.yarn.api.records.Resource usedResourceToUpdate;
  
  
  

  //PersistedEvent to persist for distributed RT
  private final List<PendingEvent> pendingEventsToAdd =
      new ArrayList<PendingEvent>();
  private RMNodeImpl rmNode = null;
  private final List<PendingEvent> persistedEventsToRemove =
      new ArrayList<PendingEvent>();

  //for debug and evaluation
  String rpcType = null;
  NodeId nodeId = null;
  private TransactionStateManager manager =null;
  
   public TransactionStateImpl(TransactionType type) {
    super(1, false);
    this.type = type;
    this.schedulerApplicationInfo =
      new SchedulerApplicationInfo(this);
  }
   
  public TransactionStateImpl(TransactionType type, int initialCounter,
          boolean batch, TransactionStateManager manager) {
    super(initialCounter, batch);
    this.type = type;
    this.schedulerApplicationInfo =
      new SchedulerApplicationInfo(this);
    if(!printerRuning){
      printerRuning = true;
      (new Thread(new LogsPrinter())).start();
    }
    this.manager = manager;
  }

  
  private static final ExecutorService executorService =
      Executors.newFixedThreadPool(50);
  
  @Override
  public void commit(boolean first) throws IOException {
    if(first){
      RMUtilities.putTransactionStateInQueues(this, rmNodesToUpdate.keySet(), appIds);
      RMUtilities.logPutInCommitingQueue(this);
    }
    executorService.execute(new RPCFinisher(this));
  }

  public FairSchedulerNodeInfo getFairschedulerNodeInfo() {
    return fairschedulerNodeInfo;
  }

  public void persistFairSchedulerNodeInfo(FSSchedulerNodeDataAccess FSSNodeDA)
      throws StorageException {
    fairschedulerNodeInfo.persist(FSSNodeDA);
  }

  public static int callsGetSchedulerApplicationInfos = 0;
  public SchedulerApplicationInfo getSchedulerApplicationInfos(ApplicationId appId) {
    if(!addAppId(appId)){
      callsGetSchedulerApplicationInfos++;
    }
    return schedulerApplicationInfo;
  }

  
  public boolean addAppId(ApplicationId appId){
    return appIds.add(appId);
  }
    
  static double totalt1 =0;
  static double totalt2 =0;
  static double totalt3 =0;
  static double totalt4 =0;
  static double totalt5 =0;
  static double totalt6 =0;
  static double totalt7=0;
    static long nbFinish =0;
    
  public static void resetLogs(){
    
    nbFinish=0;

    nbFinish=0;
    totalt1 =0;
  totalt2 =0;
  totalt3 =0;
  totalt4 =0;
  totalt5 =0;
  totalt6 =0;
  totalt7=0;
  }
      
  public void persist() throws IOException {
    Long start = System.currentTimeMillis();
    persitApplicationToAdd();
    long t1 =System.currentTimeMillis() - start;
    totalt1=totalt1 + t1;
    persistApplicationStateToRemove();
    long t2 =System.currentTimeMillis() - start;
    totalt2=totalt2 + System.currentTimeMillis() - start;
    persistAppAttempt();
    long t3 =System.currentTimeMillis() - start;
    totalt3=totalt3 + System.currentTimeMillis() - start;
    persistAllocateResponsesToAdd();
    long t4 =System.currentTimeMillis() - start;
    totalt4=totalt4 + System.currentTimeMillis() - start;
    persistAllocateResponsesToRemove();
    long t5 =System.currentTimeMillis() - start;
    totalt5=totalt5 + System.currentTimeMillis() - start;
    persistRMContainerToUpdate();
     long t6 =System.currentTimeMillis() - start;
    totalt6=totalt6 + System.currentTimeMillis() - start;
    persistContainers();
    long t7 =System.currentTimeMillis() - start;
    totalt7=totalt7 + System.currentTimeMillis() - start;
    nbFinish++;
    if(nbFinish%100==0){
    double avgt1=totalt1/nbFinish;
    double avgt2=totalt2/nbFinish;
    double avgt3=totalt3/nbFinish;
    double avgt4=totalt4/nbFinish;
    double avgt5=totalt5/nbFinish;
    double avgt6=totalt6/nbFinish;
    double avgt7=totalt7/nbFinish;
    LOG.info("avg time commit transactionStateImpl: " + avgt1 + ", " + avgt2 + ", " + avgt3 + ", " + avgt4 + ", " + avgt5 + ", " + avgt6 + ", " + avgt7);
    }
    if(t6>500){
      LOG.error("commit transactionStateImpl too long : " + t1 + ", " + t2 + ", " + t3 + ", " + t4 + ", " + t5 + ", " + t6 + ", " + t7);
    }
    //TODO rebuild cluster resource from node resources
//    persistClusterResourceToUpdate();
//    persistUsedResourceToUpdate();
  }

  public void persistSchedulerApplicationInfo(QueueMetricsDataAccess QMDA, StorageConnector connector)
      throws StorageException {
      schedulerApplicationInfo.persist(QMDA, connector);
  }

  public CSQueueInfo getCSQueueInfo() {
    return csQueueInfo;
  }

  public void persistCSQueueInfo(CSQueueDataAccess CSQDA,
          CSLeafQueueUserInfoDataAccess csLQUIDA) throws StorageException {

      csQueueInfo.persist(CSQDA, csLQUIDA);
  }
  
  public FiCaSchedulerNodeInfoToUpdate getFicaSchedulerNodeInfoToUpdate(
      String nodeId) {
    FiCaSchedulerNodeInfoToUpdate nodeInfo =
        ficaSchedulerNodeInfoToUpdate.get(nodeId);
    if (nodeInfo == null) {
      nodeInfo = new FiCaSchedulerNodeInfoToUpdate(nodeId, this);
      ficaSchedulerNodeInfoToUpdate.put(nodeId, nodeInfo);
    }
    return nodeInfo;
  }
  
  public void addFicaSchedulerNodeInfoToAdd(String nodeId,
          org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode node) {

    FiCaSchedulerNodeInfos nodeInfos = new FiCaSchedulerNodeInfos();
    String reservedContainer = node.getReservedContainer() != null ? node.
            getReservedContainer().toString() : null;
    nodeInfos.setFiCaSchedulerNode(
            new FiCaSchedulerNode(nodeId, node.getNodeName(),
                    node.getNumContainers(), reservedContainer));
    //Add Resources
    if (node.getTotalResource() != null) {
      nodeInfos.setTotalResource(new Resource(nodeId, Resource.TOTAL_CAPABILITY,
              Resource.FICASCHEDULERNODE, node.getTotalResource().getMemory(),
              node.getTotalResource().getVirtualCores(),0));
    }
    if (node.getAvailableResource() != null) {
      nodeInfos.setAvailableResource(new Resource(nodeId, Resource.AVAILABLE,
              Resource.FICASCHEDULERNODE,
              node.getAvailableResource().getMemory(),
              node.getAvailableResource().getVirtualCores(),0));
    }
    if (node.getUsedResource() != null) {
      nodeInfos.setUsedResource(
              new Resource(nodeId, Resource.USED, Resource.FICASCHEDULERNODE,
                      node.getUsedResource().getMemory(),
                      node.getUsedResource().getVirtualCores(),0));
    }
    if (node.getReservedContainer() != null) {
      addRMContainerToUpdate((RMContainerImpl)node.getReservedContainer());
    }
    
    //Add launched containers
    if (node.getRunningContainers() != null) {
      for (org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer rmContainer
              : node
              .getRunningContainers()) {
        nodeInfos.addLaunchedContainer(
                new LaunchedContainers(node.getNodeID().toString(),
                        rmContainer.getContainerId().toString(),
                        rmContainer.getContainerId().toString()));
      }
    }

    ficaSchedulerNodeInfoToAdd.put(nodeId, nodeInfos);
    ficaSchedulerNodeInfoToRemove.remove(nodeId);
    ficaSchedulerNodeInfoToUpdate.remove(nodeId);
  }
  
  public void addFicaSchedulerNodeInfoToRemove(String nodeId,
          org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode node) {
    if (ficaSchedulerNodeInfoToAdd.remove(nodeId) == null) {
      FiCaSchedulerNodeInfos nodeInfo = new FiCaSchedulerNodeInfos();
      if (node.getReservedContainer() != null) {
        nodeInfo.setReservedContainer(new RMContainer(node.
                getReservedContainer().getContainerId().toString()));
      }
      ficaSchedulerNodeInfoToRemove.put(nodeId, nodeInfo);
    }
    ficaSchedulerNodeInfoToUpdate.remove(nodeId);
  }
  
  static boolean firstApp=true;
  static public int callsAddApplicationToAdd =0;
  public void addApplicationToAdd(RMAppImpl app) {
    if(firstApp){
      firstApp=false;
      RMUtilities.resetLogs();
    }
    
    ApplicationStateDataPBImpl appStateData =
              (ApplicationStateDataPBImpl) ApplicationStateDataPBImpl
                  .newApplicationStateData(app.getSubmitTime(),
                      app.getStartTime(), app.getUser(),
                      app.getApplicationSubmissionContext(), 
                      app.getState(), 
                      app.getDiagnostics().toString(),
                      app.getFinishTime(), 
                      null);
          byte[] appStateDataBytes = appStateData.getProto().toByteArray();
          ApplicationState hop =
              new ApplicationState(app.getApplicationId().toString(),
                  appStateDataBytes, app.getUser(), app.getName(),
                  app.getState().toString());
    applicationsToAdd.put(app.getApplicationId(), hop);
    List<UpdatedNode> nodeIdsToAdd = new ArrayList<UpdatedNode>();
    for(NodeId nid: app.getUpdatedNodesId()){
      UpdatedNode node = new UpdatedNode(app.getApplicationId().toString(),
               nid.toString());
      nodeIdsToAdd.add(node);
    }
    updatedNodeIdToAdd.put(app.getApplicationId(), nodeIdsToAdd);
    applicationsStateToRemove.remove(app.getApplicationId());
    callsAddApplicationToAdd++;
    addAppId(app.getApplicationId());
  }
  
  private void persitApplicationToAdd() throws IOException {
    if (!applicationsToAdd.isEmpty()) {
      ApplicationStateDataAccess DA =
          (ApplicationStateDataAccess) RMStorageFactory
              .getDataAccess(ApplicationStateDataAccess.class);
      DA.addAll(applicationsToAdd.values());
      
      UpdatedNodeDataAccess uNDA = (UpdatedNodeDataAccess)RMStorageFactory
              .getDataAccess(UpdatedNodeDataAccess.class);
      uNDA.addAll(updatedNodeIdToAdd.values());
    }
  }

  private String dumpApplicationToAdd() {
    if (!applicationsToAdd.isEmpty()) {
      String result = "application to add: " + applicationsToAdd.values().size()
              + "\n";
      int total = 0;
      for (List<UpdatedNode> up : updatedNodeIdToAdd.values()) {
        total += up.size();
      }
      int avg = total / updatedNodeIdToAdd.size();
      result = result + "updatedNodeIdToAdd: " + updatedNodeIdToAdd.values().
              size() + " avg list size: " + avg + "\n";
      return result;
    } else {
      return "";
    }
  }
  
  public static int callsAddApplicationStateToRemove = 0;
  public void addApplicationStateToRemove(ApplicationId appId) {
    if(applicationsToAdd.remove(appId)==null){
    updatedNodeIdToAdd.remove(appId);
    applicationsStateToRemove.add(appId);
    }
    callsAddApplicationStateToRemove++;
    addAppId(appId);
  }

  private void persistApplicationStateToRemove() throws StorageException {
    if (!applicationsStateToRemove.isEmpty()) {
      ApplicationStateDataAccess DA =
          (ApplicationStateDataAccess) RMStorageFactory
              .getDataAccess(ApplicationStateDataAccess.class);
      List<ApplicationState> appToRemove = new ArrayList<ApplicationState>();
      for (ApplicationId appId : applicationsStateToRemove) {
        appToRemove.add(new ApplicationState(appId.toString()));
      }
      DA.removeAll(appToRemove);
      //TODO remove appattempts
    }
  }
  
  public static int callsaddAppAttempt = 0;
  public void addAppAttempt(RMAppAttempt appAttempt) {
    String appIdStr = appAttempt.getAppAttemptId().getApplicationId().
            toString();

    Credentials credentials = appAttempt.getCredentials();
    ByteBuffer appAttemptTokens = null;

    if (credentials != null) {
      DataOutputBuffer dob = new DataOutputBuffer();
      try {
        credentials.writeTokenStorageToStream(dob);
      } catch (IOException ex) {
        LOG.error("faillerd to persist tocken: " + ex, ex);
      }
      appAttemptTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
    }
    List<ContainerStatus> justFinishedContainers = 
            new ArrayList<ContainerStatus>(appAttempt.getJustFinishedContainers());
    ApplicationAttemptStateDataPBImpl attemptStateData
            = (ApplicationAttemptStateDataPBImpl) ApplicationAttemptStateDataPBImpl.
            newApplicationAttemptStateData(appAttempt.getAppAttemptId(),
                    appAttempt.getMasterContainer(), appAttemptTokens,
                    appAttempt.getStartTime(), appAttempt.
                    getState(), appAttempt.getOriginalTrackingUrl(),
                    appAttempt.getDiagnostics(),
                    appAttempt.getFinalApplicationStatus(),
                    new HashSet<NodeId>(),
                    justFinishedContainers,
                    appAttempt.getProgress(), appAttempt.getHost(),
                    appAttempt.getRpcPort());

    byte[] attemptStateByteArray = attemptStateData.getProto().toByteArray();
    if(attemptStateByteArray.length > 13000){
      LOG.error("application Attempt State too big: " + appAttempt.getAppAttemptId() + " " + appAttemptTokens.array().length + " " + 
              appAttempt.getDiagnostics().getBytes().length + " " + appAttempt.getRanNodes().size() + " " + appAttempt.getJustFinishedContainers().size() +
              " ");
    }
    this.appAttempts.put(appAttempt.getAppAttemptId().toString(),
            new ApplicationAttemptState(appIdStr, appAttempt.getAppAttemptId().
                    toString(),
                    attemptStateByteArray, appAttempt.
                    getHost(), appAttempt.getRpcPort(), appAttemptTokens,
                    appAttempt.
                    getTrackingUrl()));
    callsaddAppAttempt++;
    addAppId(appAttempt.getAppAttemptId().getApplicationId());
  }
  
  public void addAllRanNodes(RMAppAttempt appAttempt) {
    Map<Integer, RanNode> ranNodeToPersist = new HashMap<Integer, RanNode>();
    List<NodeId> ranNodes = new ArrayList<NodeId>(appAttempt.getRanNodes());
    for (NodeId nid : ranNodes) {
      RanNode node = new RanNode(appAttempt.getAppAttemptId().toString(),
                      nid.toString());
      ranNodeToPersist.put(node.hashCode(),node);
    }

    this.ranNodeToAdd.put(appAttempt.getAppAttemptId(),
            ranNodeToPersist);
  }

  public void addRanNode(NodeId nid, ApplicationAttemptId appAttemptId) {
    if(!this.ranNodeToAdd.containsKey(appAttemptId)){
      this.ranNodeToAdd.put(appAttemptId, new HashMap<Integer,RanNode>());
    }
    RanNode node = new RanNode(appAttemptId.toString(), nid.toString());
    this.ranNodeToAdd.get(appAttemptId).put(node.hashCode(),node);
  }
  
  private void persistAppAttempt() throws IOException {
    if (!appAttempts.isEmpty()) {

      ApplicationAttemptStateDataAccess DA =
          (ApplicationAttemptStateDataAccess) RMStorageFactory.
              getDataAccess(ApplicationAttemptStateDataAccess.class);
      DA.addAll(appAttempts.values());
      
      RanNodeDataAccess rDA= (RanNodeDataAccess) RMStorageFactory.
              getDataAccess(RanNodeDataAccess.class);
      rDA.addAll(ranNodeToAdd.values());
    }
  }
  
  private String dumpAppAttempt() {
    if (!appAttempts.isEmpty()) {
      int stateSize = 0;
      int tokenSize = 0;
      for (ApplicationAttemptState state : appAttempts.values()) {
        stateSize += state.getApplicationattemptstate().length;
        tokenSize += state.getAppAttemptTokens().array().length;
      }
      int avgStateSize = stateSize / appAttempts.values().size();
      int avgTokenSize = tokenSize/appAttempts.values().size();
      
      return "appAttempt : " + appAttempts.values().size() + " avg state size: "
              + avgStateSize + " avg token size: " + avgTokenSize;
    }
    return "";
  }
  
  public static int callsaddAllocateResponse=0;
  public void addAllocateResponse(ApplicationAttemptId id,
          AllocateResponseLock allocateResponse) {
    AllocateResponsePBImpl lastResponse
            = (AllocateResponsePBImpl) allocateResponse.
            getAllocateResponse();
    if (lastResponse != null) {
      List<String> allocatedContainers = new ArrayList<String>();
      for(Container container: lastResponse.getAllocatedContainers()){
        allocatedContainers.add(container.getId().toString());
      }
      
      AllocateResponsePBImpl toPersist = new AllocateResponsePBImpl();
      toPersist.setAMCommand(lastResponse.getAMCommand());
      toPersist.setAvailableResources(lastResponse.getAvailableResources());
      toPersist.setCompletedContainersStatuses(lastResponse.getCompletedContainersStatuses());
      toPersist.setDecreasedContainers(lastResponse.getDecreasedContainers());
      toPersist.setIncreasedContainers(lastResponse.getIncreasedContainers());
//      toPersist.setNMTokens(lastResponse.getNMTokens());
      toPersist.setNumClusterNodes(lastResponse.getNumClusterNodes());
      toPersist.setPreemptionMessage(lastResponse.getPreemptionMessage());
      toPersist.setResponseId(lastResponse.getResponseId());
      toPersist.setUpdatedNodes(lastResponse.getUpdatedNodes());
      
      this.allocateResponsesToAdd.put(id, new AllocateResponse(id.toString(),
              toPersist.getProto().toByteArray(), allocatedContainers, 
      allocateResponse.getAllocateResponse().getResponseId()));
      if(toPersist.getProto().toByteArray().length>1000){
        LOG.info("add allocateResponse of size " + toPersist.getProto().toByteArray().length + 
                " for " + id + " content: " + print(toPersist));
      }
      allocateResponsesToRemove.remove(id);
      callsaddAllocateResponse++;
      addAppId(id.getApplicationId());
    }
  }
  
  private String print(org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse response){
    String s ="";
    if(response.getAMCommand()!= null)
      s = s + "AM comande : " + response.getAMCommand().toString();
    if(response.getAllocatedContainers()!=null)
    s = s + " allocated containers size " + response.getAllocatedContainers().size();
    if(response.getCompletedContainersStatuses()!=null)
    s = s + " completed containersStatuses size " + response.getCompletedContainersStatuses().size();
    if(response.getDecreasedContainers()!=null)
    s = s + " decreasedcont: " + response.getDecreasedContainers().size();
    if(response.getIncreasedContainers()!= null)
    s = s + " increased containers: " + response.getIncreasedContainers().size();
    if(response.getNMTokens()!= null)
    s = s + " nmtokens " + response.getNMTokens().size();
    if(response.getUpdatedNodes()!=null)
    s =s + " updatedNodes " + response.getUpdatedNodes().size();
    return s;
  }

  static double nbPersist =0;
  static double tt1=0;
  static double tt2=0;
  static double tt3=0;
  private void persistAllocateResponsesToAdd() throws IOException {
    if (!allocateResponsesToAdd.isEmpty()) {
      long start = System.currentTimeMillis();
      AllocateResponseDataAccess da =
          (AllocateResponseDataAccess) RMStorageFactory
              .getDataAccess(AllocateResponseDataAccess.class);
      AllocatedContainersDataAccess containersDA = (AllocatedContainersDataAccess)
              RMStorageFactory.getDataAccess(AllocatedContainersDataAccess.class);
      da.update(allocateResponsesToAdd.values());
      tt1 = tt1 + System.currentTimeMillis() - start;
      containersDA.update(allocateResponsesToAdd.values());
      tt2 = tt2 + System.currentTimeMillis() - start;
      tt3 = tt3 + System.currentTimeMillis() - start;
      nbPersist++;
      if(nbPersist%100 == 0){
        double avgt1 = tt1/nbPersist;
        double avgt2 = tt2/nbPersist;
        double avgt3 = tt3/nbPersist;
        LOG.info("persist allocate response to add avg time: " + avgt1 + ", " + avgt2 + ", " + avgt3);
      }
    }
  }
  
  private String dumpAllocateResponsesToAdd(){
    if(! allocateResponsesToAdd.isEmpty()){
      int responseSize = 0;
      int allocatedContainersSize = 0;
      for(AllocateResponse response : allocateResponsesToAdd.values()){
        responseSize+= response.getAllocateResponse().length;
        allocatedContainersSize+=response.getAllocatedContainers().size();
      }
      int avgResponseSize = responseSize/allocateResponsesToAdd.size();
      int avgAllocatedContainersSize = allocatedContainersSize/allocateResponsesToAdd.size();
      return "allocatedResponseTo add: " + allocateResponsesToAdd.size() + 
              " avg response size: " + avgResponseSize + 
              " avg allocated containers size" + avgAllocatedContainersSize;
    }
    return "";
  }
  
  public static int callremoveAllocateResponse=0;
  public void removeAllocateResponse(ApplicationAttemptId id, int responseId) {
    if(allocateResponsesToAdd.remove(id)==null){
    this.allocateResponsesToRemove.add(new AllocateResponse(id.toString(), responseId));
    }
    callremoveAllocateResponse++;
    addAppId(id.getApplicationId());
  }
  
  private void persistAllocateResponsesToRemove() throws IOException {
    if (!allocateResponsesToRemove.isEmpty()) {
      AllocateResponseDataAccess da =
          (AllocateResponseDataAccess) RMStorageFactory
              .getDataAccess(AllocateResponseDataAccess.class);

      da.removeAll(allocateResponsesToRemove);
    }
  }
  
   private byte[] getRMContainerBytes(org.apache.hadoop.yarn.api.records.Container Container){
    if(Container instanceof ContainerPBImpl){
      return ((ContainerPBImpl) Container).getProto()
            .toByteArray();
    }else{
      return new byte[0];
    }
  }
   
  public void addRMContainerToAdd(RMContainerImpl rmContainer) {
    addRMContainerToUpdate(rmContainer);
    io.hops.metadata.yarn.entity.Container hopContainer
            = new io.hops.metadata.yarn.entity.Container(rmContainer.
                    getContainerId().
                    toString(),
                    getRMContainerBytes(rmContainer.getContainer()));
    toAddContainers.add(hopContainer);
  }
  
  protected void persistContainers() throws StorageException {
    if (!toAddContainers.isEmpty()) {
      ContainerDataAccess cDA = (ContainerDataAccess) RMStorageFactory.
              getDataAccess(ContainerDataAccess.class);
      cDA.addAll(toAddContainers);
    }
  }
  
  public void addRMContainerToUpdate(RMContainerImpl rmContainer) {
    boolean isReserved = (rmContainer.getReservedNode() != null)
            && (rmContainer.getReservedPriority() != null);

    String reservedNode = isReserved ? rmContainer.getReservedNode().toString()
            : null;
    int reservedPriority = isReserved ? rmContainer.getReservedPriority().
            getPriority() : 0;
    int reservedMemory = isReserved ? rmContainer.getReservedResource().
            getMemory() : 0;

    int reservedVCores = isReserved ? rmContainer.getReservedResource().
            getVirtualCores() : 0;
    
    rmContainersToUpdate
            .put(rmContainer.getContainer().getId().toString(), new RMContainer(
                            rmContainer.getContainer().getId().toString(),
                            rmContainer.getApplicationAttemptId().toString(),
                            rmContainer.getNodeId().toString(),
                            rmContainer.getUser(),
                            reservedNode,
                            reservedPriority,
                            reservedMemory,
                            reservedVCores,
                            rmContainer.getStartTime(),
                            rmContainer.getFinishTime(),
                            rmContainer.getState().toString(),
                            rmContainer.getContainerState().toString(),
                            rmContainer.getContainerExitStatus()));
  }

  private void persistRMContainerToUpdate() throws StorageException {
    if (!rmContainersToUpdate.isEmpty()) {
      RMContainerDataAccess rmcontainerDA =
          (RMContainerDataAccess) RMStorageFactory
              .getDataAccess(RMContainerDataAccess.class);      
      rmcontainerDA.addAll(rmContainersToUpdate.values());
    }
  }

  private String dumpRMContainerToUpdate(){
    return "rmContainer to update " + rmContainersToUpdate.size();
  }
  
  public void persistFicaSchedulerNodeInfo(ResourceDataAccess resourceDA,
      FiCaSchedulerNodeDataAccess ficaNodeDA,
      RMContainerDataAccess rmcontainerDA,
      LaunchedContainersDataAccess launchedContainersDA)
      throws StorageException {
    persistFiCaSchedulerNodeToAdd(resourceDA, ficaNodeDA, rmcontainerDA,
        launchedContainersDA);
    FiCaSchedulerNodeInfoAgregate agregate = new FiCaSchedulerNodeInfoAgregate();
    for (FiCaSchedulerNodeInfoToUpdate nodeInfo : ficaSchedulerNodeInfoToUpdate
        .values()) {
      nodeInfo.agregate(agregate);
    }
    agregate.persist(resourceDA, ficaNodeDA, rmcontainerDA, launchedContainersDA);
    persistFiCaSchedulerNodeToRemove(resourceDA, ficaNodeDA, rmcontainerDA, launchedContainersDA);
  }

  public RMContextInfo getRMContextInfo() {
    return rmcontextInfo;
  }

  public void persistRmcontextInfo(RMNodeDataAccess rmnodeDA,
      ResourceDataAccess resourceDA, NodeDataAccess nodeDA,
      RMContextInactiveNodesDataAccess rmctxinactivenodesDA)
      throws StorageException {
    rmcontextInfo.persist(rmnodeDA, resourceDA, nodeDA, rmctxinactivenodesDA);
  }


  public void persistRMNodeToUpdate(RMNodeDataAccess rmnodeDA)
      throws StorageException {
    if (!rmNodesToUpdate.isEmpty()) {
      rmnodeDA.addAll(rmNodesToUpdate.values());
    }
  }

  public void toUpdateRMNode(
      org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode rmnodeToAdd) {
      RMNode hopRMNode = new RMNode(rmnodeToAdd.getNodeID().toString(),
          rmnodeToAdd.getHostName(), rmnodeToAdd.getCommandPort(),
          rmnodeToAdd.getHttpPort(), rmnodeToAdd.getNodeAddress(),
          rmnodeToAdd.getHttpAddress(), rmnodeToAdd.getHealthReport(),
          rmnodeToAdd.getLastHealthReportTime(),
          ((RMNodeImpl) rmnodeToAdd).getCurrentState(),
          rmnodeToAdd.getNodeManagerVersion(), -1,
            ((RMNodeImpl) rmnodeToAdd).getUpdatedContainerInfoId(), rmnodeToAdd.
            getRMNodePendingEventId());
    this.rmNodesToUpdate.put(rmnodeToAdd.getNodeID().toString(), hopRMNode);
  }

  public Map<String, RMNode> getRMNodesToUpdate(){
    return rmNodesToUpdate;
  }
  
  public RMNodeInfo getRMNodeInfo(NodeId rmNodeId) {
    RMNodeInfo result = rmNodeInfos.get(rmNodeId);
    if (result == null) {
      result = new RMNodeInfo(rmNodeId.toString());
      rmNodeInfos.put(rmNodeId, result);
    }
    return result;
  }

  public void persistRMNodeInfo(NodeHBResponseDataAccess hbDA,
      ContainerIdToCleanDataAccess cidToCleanDA,
      JustLaunchedContainersDataAccess justLaunchedContainersDA,
          UpdatedContainerInfoDataAccess updatedContainerInfoDA,
          FinishedApplicationsDataAccess faDA, ContainerStatusDataAccess csDA,
          PendingEventDataAccess persistedEventsDA)
          throws StorageException {
    if (rmNodeInfos != null) {
      RMNodeInfoAgregate agregate = new RMNodeInfoAgregate();
      for (RMNodeInfo rmNodeInfo : rmNodeInfos.values()) {
        rmNodeInfo.agregate(agregate);
      }
      agregate.persist(hbDA, cidToCleanDA, justLaunchedContainersDA,
              updatedContainerInfoDA, faDA, csDA,persistedEventsDA);
    }
  }
  
  public void updateUsedResource(
      org.apache.hadoop.yarn.api.records.Resource usedResource) {
    this.usedResourceToUpdate = usedResource;
  }
  
  private void persistUsedResourceToUpdate() throws StorageException {
    if (usedResourceToUpdate != null) {
      ResourceDataAccess rDA = (ResourceDataAccess) RMStorageFactory
          .getDataAccess(ResourceDataAccess.class);
      rDA.add(new Resource("cluster", Resource.CLUSTER, Resource.USED,
          usedResourceToUpdate.getMemory(),
          usedResourceToUpdate.getVirtualCores(),0));
    }
  }
  
  public void updateClusterResource(
      org.apache.hadoop.yarn.api.records.Resource clusterResource) {
    this.clusterResourceToUpdate = clusterResource;
  }
  
  private void persistClusterResourceToUpdate() throws StorageException {
    if (clusterResourceToUpdate != null) {
      ResourceDataAccess rDA = (ResourceDataAccess) RMStorageFactory
          .getDataAccess(ResourceDataAccess.class);
      rDA.add(new Resource("cluster", Resource.CLUSTER, Resource.AVAILABLE,
          clusterResourceToUpdate.getMemory(),
          clusterResourceToUpdate.getVirtualCores(),0));
    }
  }

  private void persistFiCaSchedulerNodeToRemove(ResourceDataAccess resourceDA, FiCaSchedulerNodeDataAccess ficaNodeDA, RMContainerDataAccess rmcontainerDA, LaunchedContainersDataAccess launchedContainersDA) throws StorageException {
    if (!ficaSchedulerNodeInfoToRemove.isEmpty()) {
      ArrayList<FiCaSchedulerNode> toRemoveFiCaSchedulerNodes =
          new ArrayList<FiCaSchedulerNode>();
//      ArrayList<Resource> toRemoveResources = new ArrayList<Resource>();
      ArrayList<RMContainer> rmcontainerToRemove = new ArrayList<RMContainer>();
      for (String nodeId : ficaSchedulerNodeInfoToRemove.keySet()) {
        toRemoveFiCaSchedulerNodes.add(new FiCaSchedulerNode(nodeId));
        //Remove Resources
        //Set memory and virtualcores to zero as we do not need
        //these values during remove anyway.
//        toRemoveResources.add(new Resource(nodeId, Resource.TOTAL_CAPABILITY,
//            Resource.FICASCHEDULERNODE, 0, 0));
//        toRemoveResources.add(
//            new Resource(nodeId, Resource.AVAILABLE, Resource.FICASCHEDULERNODE,
//                0, 0));
//        toRemoveResources.add(
//            new Resource(nodeId, Resource.USED, Resource.FICASCHEDULERNODE, 0,
//                0));
        // Update FiCaSchedulerNode reservedContainer
        RMContainer container =
            ficaSchedulerNodeInfoToRemove.get(nodeId).getReservedContainer();
        if (container != null) {
          rmcontainerToRemove.add(container);
        }
      }
//      resourceDA.removeAll(toRemoveResources);
      ficaNodeDA.removeAll(toRemoveFiCaSchedulerNodes);
      rmcontainerDA.removeAll(rmcontainerToRemove);
    }
  }

  public void persistFiCaSchedulerNodeToAdd(ResourceDataAccess resourceDA,
          FiCaSchedulerNodeDataAccess ficaNodeDA,
          RMContainerDataAccess rmcontainerDA,
          LaunchedContainersDataAccess launchedContainersDA)
          throws StorageException {
    if (!ficaSchedulerNodeInfoToAdd.isEmpty()) {
      ArrayList<FiCaSchedulerNode> toAddFiCaSchedulerNodes
              = new ArrayList<FiCaSchedulerNode>();
      ArrayList<Resource> toAddResources = new ArrayList<Resource>();
      ArrayList<RMContainer> rmcontainerToAdd = new ArrayList<RMContainer>();
      ArrayList<LaunchedContainers> toAddLaunchedContainers
              = new ArrayList<LaunchedContainers>();
      for (FiCaSchedulerNodeInfos nodeInfo : ficaSchedulerNodeInfoToAdd.values()) {

        toAddFiCaSchedulerNodes.add(nodeInfo.getFiCaSchedulerNode());
        //Add Resources
        if (nodeInfo.getTotalResource() != null) {
          toAddResources.add(nodeInfo.getTotalResource());
        }
        if (nodeInfo.getAvailableResource() != null) {
          toAddResources.add(nodeInfo.getAvailableResource());
        }
        if (nodeInfo.getUsedResource() != null) {
          toAddResources.add(nodeInfo.getUsedResource());
        }
        //Add launched containers
        if (nodeInfo.getLaunchedContainers() != null) {
          toAddLaunchedContainers.addAll(nodeInfo.getLaunchedContainers());
        }
      }
      resourceDA.addAll(toAddResources);
      ficaNodeDA.addAll(toAddFiCaSchedulerNodes);
      launchedContainersDA.addAll(toAddLaunchedContainers);
    }
  }

  public RMNodeImpl getRMNode() {
    return this.rmNode;
  }

  /**
   * Remove pending event from DB. In this case, the event id is not needed,
   * hence set to MIN.
   * <p/>
   *
   * @param id
   * @param rmnodeId
   * @param type
   * @param status
   */
  public void addPendingEventToRemove(int id, String rmnodeId, int type,
      int status) {
    this.persistedEventsToRemove
        .add(new PendingEvent(rmnodeId, type, status, id));
  }


  static List<Long> durations = new ArrayList<Long>();
  static boolean printerRuning = false;
  
  private class RPCFinisher implements Runnable {

    private final TransactionStateImpl ts;

    public RPCFinisher(TransactionStateImpl ts) {
      this.ts = ts;
    }

    public void run() {
      try{
        RMUtilities.finishRPCs(ts);
      }catch(IOException ex){
        LOG.error("did not commit state properly", ex);
    }
  }
}
  
  
  
  public void dump(){
    String dump = "";
    dump = dump + dumpApplicationToAdd() + "\n";
    dump = dump + dumpAppAttempt() + "\n";
    dump = dump + dumpAllocateResponsesToAdd() + "\n";
    dump = dump + dumpRMContainerToUpdate();
  }
  
  public TransactionStateManager getManager(){
    return manager;
  }
}
