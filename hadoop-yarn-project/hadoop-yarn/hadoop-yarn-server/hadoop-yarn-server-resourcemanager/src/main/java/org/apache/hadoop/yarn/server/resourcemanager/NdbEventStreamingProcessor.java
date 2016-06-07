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
package org.apache.hadoop.yarn.server.resourcemanager;

import io.hops.exception.StorageException;
import io.hops.metadata.util.RMUtilities;
import io.hops.metadata.yarn.entity.ContainerStatus;
import io.hops.metadata.yarn.entity.JustLaunchedContainers;
import io.hops.metadata.yarn.entity.RMNodeComps;
import io.hops.metadata.yarn.entity.UpdatedContainerInfo;
import java.io.IOException;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import io.hops.metadata.yarn.entity.appmasterrpc.RPC;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.KillApplicationRequestPBImpl;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.exceptions.ApplicationNotFoundException;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import io.hops.ha.common.TransactionState;
import io.hops.ha.common.TransactionStateImpl;
import io.hops.metadata.util.HopYarnAPIUtilities;
import io.hops.metadata.util.RMStorageFactory;
import io.hops.metadata.yarn.dal.YarnApplicationsQuotaDataAccess;
import io.hops.metadata.yarn.dal.YarnApplicationsToKillDataAccess;
import io.hops.metadata.yarn.dal.util.YARNOperationType;
import io.hops.metadata.yarn.entity.YarnApplicationsQuota;
import io.hops.metadata.yarn.entity.YarnApplicationsToKill;
import io.hops.transaction.handler.LightWeightRequestHandler;
import java.util.ArrayList;
import java.util.Map;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEventType;

/**
 *
 * @author sri
 */
public class NdbEventStreamingProcessor extends PendingEventRetrieval {

  public NdbEventStreamingProcessor(RMContext rmContext, Configuration conf) {
    super(rmContext, conf);

  }

  public void printHopsRMNodeComps(RMNodeComps hopRMNodeNDBCompObject) {

    //print hoprmnode 
    LOG.debug(
            "<EvtProcessor_PRINT_START>-------------------------------------------------------------------");
    if (hopRMNodeNDBCompObject.getHopRMNode() != null) {
      LOG.debug("<EvtProcessor> [rmnode] id : " + hopRMNodeNDBCompObject.
              getHopRMNode().getNodeId() + "| peinding id : "
              + hopRMNodeNDBCompObject.getHopRMNode().getPendingEventId());
    }
    //print hopnode
    if (hopRMNodeNDBCompObject.getHopNode() != null) {
      LOG.debug("<EvtProcessor> [node] id : " + hopRMNodeNDBCompObject.
              getHopNode().getId() + "| level : " + hopRMNodeNDBCompObject.
              getHopNode().getLevel());
    }
    //print hopresource 
    if (hopRMNodeNDBCompObject.getHopResource() != null) {
      LOG.debug("<EvtProcessor> [resource] id : " + hopRMNodeNDBCompObject.
              getHopResource().getId() + "| memory : " + hopRMNodeNDBCompObject.
              getHopResource().getMemory());
    }
    if (hopRMNodeNDBCompObject.getPendingEvent() != null) {
      LOG.debug("<EvtProcessor> [pendingevent] id : " + hopRMNodeNDBCompObject.
              getPendingEvent().getId().getNodeId() + "| peinding id : "
              + hopRMNodeNDBCompObject.getPendingEvent().getId() +
              "| type: " + hopRMNodeNDBCompObject.getPendingEvent().getType() +
              "| status: " + hopRMNodeNDBCompObject.getPendingEvent().getStatus());
    }
    List<UpdatedContainerInfo> hopUpdatedContainerInfo = hopRMNodeNDBCompObject.
            getHopUpdatedContainerInfo();
    for (UpdatedContainerInfo hopuc : hopUpdatedContainerInfo) {
      LOG.debug("<EvtProcessor> [updatedcontainerinfo] id : " + hopuc.
              getRmnodeid() + "| container id : " + hopuc.getContainerId());
    }
    List<ContainerStatus> hopContainersStatus = hopRMNodeNDBCompObject.
            getHopContainersStatus();
    for (ContainerStatus hopCS : hopContainersStatus) {
      LOG.debug("<EvtProcessor> [containerstatus] id : " + hopCS.getRMNodeId()
              + "| container id : " + hopCS.getContainerid()
              + "| container status : " + hopCS.getExitstatus());
    }
    LOG.debug(
            "<EvtProcessor_PRINT_END>-------------------------------------------------------------------");
  }

  public void start() {
    if (!active) {
      active = true;
      LOG.info("start retriving thread");
      retrivingThread = new Thread(new RetrivingThread());
      retrivingThread.setName("event retriver");
      retrivingThread.start();
    } else {
      LOG.error("ndb event retriver is already active");
    }
  }
  
  private class RetrivingThread implements Runnable {

    @Override
    public void run() {
      while (active) {
        try {
          //first ask him sleep

          RMNodeComps hopRMNodeCompObject = null;
          hopRMNodeCompObject
                  = (RMNodeComps) NdbEventStreamingReceiver.blockingQueue.take();
          if (hopRMNodeCompObject != null) {
            if (LOG.isDebugEnabled()) {
              printHopsRMNodeComps(hopRMNodeCompObject);
            }
            if (rmContext.isDistributedEnabled()) {
              RMNode rmNode = null;
              try {
                rmNode = RMUtilities.processHopRMNodeCompsForScheduler(
                        hopRMNodeCompObject,
                        rmContext);
                LOG.debug("HOP :: RMNodeWorker rmNode:" + rmNode);

                if (rmNode != null) {
                  updateRMContext(rmNode);
                  triggerEvent(rmNode, hopRMNodeCompObject.getPendingEvent(),
                          false);
                }
              } catch (Exception ex) {
                LOG.error("HOP :: Error retrieving rmNode:" + ex, ex);
              }
            }
            // Processes container statuses for ContainersLogs service
            List<ContainerStatus> hopContainersStatusList
                    = hopRMNodeCompObject.getHopContainersStatus();
            if (rmContext.isLeadingRT() && hopContainersStatusList.size() > 0 &&
                    rmContext.getContainersLogsService() !=null) {
              rmContext.getContainersLogsService()
                      .insertEvent(hopContainersStatusList);
            }
            // Kill the applications form the applications_to_be_killed list
            List<String> appsToKill = hopRMNodeCompObject.getHopApplicationsToKillList();
            //The list can be null which would result in a npe
            for (String app: appsToKill){
                // kill the app
                LOG.debug("RIZ:: Killing app " + app); 
                try {
                  KillApplication(app); 
                  
                } catch (Exception e) {
                   LOG.error(e); 
                }
                
            }
            ClearApplicationsRecordes(appsToKill);
          }
        } catch (InterruptedException ex) {
          LOG.error(ex, ex);
        }

      }

    }
    
    //we should look at this together 
    private void KillApplication(String app) throws Exception{
        //String app = application.getApplicationId().toString();
        String parts[] = app.split("_");
        LOG.info("RIZ:: " + parts[1] + " " + parts[2]);
        
        // Get ApplicationId and KillApplicationRequest
        ApplicationId applicationId =  ApplicationId.newInstance(Long.parseLong(parts[1],10) ,Integer.parseInt(parts[2]));
        //ApplicationId applicationId = application.getApplicationId();
        KillApplicationRequest killRequest = KillApplicationRequest.newInstance(applicationId);
        
                        
        TransactionState transactionState = rmContext.getTransactionStateManager().getCurrentTransactionStateNonPriority(-1,"forceKillApplication");
        ((TransactionStateImpl) transactionState).addApplicationToKillToRemove(applicationId);
        RMApp application = rmContext.getRMApps().get(applicationId);
        
        if (application == null) {
          RMAuditLogger.logFailure("Quota service", RMAuditLogger.AuditConstants.KILL_APP_REQUEST,"UNKNOWN", "ClientRMService", "Trying to kill an absent application", applicationId);
          transactionState.decCounter(TransactionState.TransactionType.INIT);
          throw new ApplicationNotFoundException("Trying to kill an absent" + " application " + applicationId);
        }
        
        rmContext.getDispatcher().getEventHandler().handle(new RMAppEvent(applicationId, RMAppEventType.KILL, transactionState));
        // For UnmanagedAMs, return true so they don't retry
        transactionState.decCounter(TransactionState.TransactionType.INIT);
        
    }

//    private void ClearApplicationsRecordes(List<String> apps) {
//      final List<YarnApplicationsToKill> appsKilled = new ArrayList<YarnApplicationsToKill>();
//      final List<YarnApplicationsQuota> appsKilledQt = new ArrayList<YarnApplicationsQuota>();                            
//                            
//      for(String app: apps){
//        appsKilled.add(new YarnApplicationsToKill(app));
//        appsKilledQt.add(new YarnApplicationsQuota(app));
//      }
//      
//      final YarnApplicationsToKillDataAccess<YarnApplicationsToKill> appsKilledDA = (YarnApplicationsToKillDataAccess) RMStorageFactory.getDataAccess(YarnApplicationsToKillDataAccess.class);
//      final YarnApplicationsQuotaDataAccess<YarnApplicationsQuota> appsKilledQtDA  = (YarnApplicationsQuotaDataAccess) RMStorageFactory.getDataAccess(YarnApplicationsQuotaDataAccess.class);
//      
//      try {
//      LightWeightRequestHandler clearApplicationsHandler;
//        clearApplicationsHandler
//                = new LightWeightRequestHandler(YARNOperationType.TEST) {
//                  @Override
//                  public Object performTask() throws StorageException {
//                    connector.beginTransaction();
//                    connector.writeLock();
//                    
//                    appsKilledDA.removeAll(appsKilled);
//                    appsKilledQtDA.removeAll(appsKilledQt);
//
//                    connector.commit();
//                    return null;
//                  }
//                };
//      clearApplicationsHandler.handle();
//    } catch (IOException ex) {
//      LOG.warn("Unable to retrieve container statuses", ex);
//    }
//      
//    }
    
  }

}
