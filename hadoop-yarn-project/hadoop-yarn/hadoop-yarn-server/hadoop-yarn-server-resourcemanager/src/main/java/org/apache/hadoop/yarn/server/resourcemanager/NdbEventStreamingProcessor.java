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

import io.hops.metadata.yarn.entity.ContainerId;
import io.hops.metadata.yarn.entity.ContainerStatus;
import io.hops.metadata.yarn.entity.FinishedApplications;
import io.hops.metadata.yarn.entity.JustLaunchedContainers;
import io.hops.metadata.yarn.entity.RMNodeComps;
import io.hops.metadata.yarn.entity.UpdatedContainerInfo;
import java.io.IOException;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
/**
 *
 * @author sri
 */
public class NdbEventStreamingProcessor extends PendingEventRetrieval {

    private RMNode rmNode;

    
    public NdbEventStreamingProcessor(RMContext rmContext, Configuration conf) {
        super(rmContext, conf);

    }

    public void printHopsRMNodeComps(RMNodeComps hopRMNodeNDBCompObject) {

        //print hoprmnode 
        if (hopRMNodeNDBCompObject.getHopRMNode() != null) {
            LOG.debug("<HopRMNode> Host name  :" + hopRMNodeNDBCompObject.getHopRMNode().getHostName()+
                    "node id : "+hopRMNodeNDBCompObject.getHopRMNode().getNodeId()+
                    "current state :"+hopRMNodeNDBCompObject.getHopRMNode().getCurrentState());
        } else {
            LOG.debug("<HopRMNode> is null");
        }
        //print hopnode
        if (hopRMNodeNDBCompObject.getHopNode() != null) {
            LOG.debug("<HopNode> RMnode id :" + hopRMNodeNDBCompObject.getHopNode().getId() + "| Location : " + hopRMNodeNDBCompObject.getHopNode().getLocation());
        }
        //print nexthearbeat
        if (hopRMNodeNDBCompObject.getHopNextHeartbeat() != null) {
            LOG.debug("<HopNextHeartbeat> RMNodeid :" + hopRMNodeNDBCompObject.getHopNextHeartbeat().getRmnodeid() + "|NextHeartBeat : " + hopRMNodeNDBCompObject.getHopNextHeartbeat().isNextheartbeat());

        }
        //print hopresource 
        if (hopRMNodeNDBCompObject.getHopResource() != null) {
            LOG.debug("<HopResource> memory:" + hopRMNodeNDBCompObject.getHopResource().getMemory() + "| Type  : " + hopRMNodeNDBCompObject.getHopResource().getType());
        }
        // print hbresponse
        if (hopRMNodeNDBCompObject.getHopNodeHBResponse() != null) {
            String hbResponse = new String(hopRMNodeNDBCompObject.getHopNodeHBResponse().getResponse());
            LOG.debug("<HopHBResponse> RMNodeId :" + hopRMNodeNDBCompObject.getHopNodeHBResponse().getRMNodeId() + "| Response  : " + hbResponse);
        }

        if (hopRMNodeNDBCompObject.getPendingEvent() != null) {
            LOG.debug("<HopPendingEvent> rm node id " + hopRMNodeNDBCompObject.getPendingEvent().getRmnodeId() + "id unique to rmnode :" + hopRMNodeNDBCompObject.getPendingEvent().getId());
        }

        List<JustLaunchedContainers> hopJustLaunchedContainers = hopRMNodeNDBCompObject.getHopJustLaunchedContainers();
        for (JustLaunchedContainers hopjl : hopJustLaunchedContainers) {
            LOG.debug("<HopJustLaunchedContainers> <List> RMNodeid:" + hopjl.getRmnodeid() + "| ContainerID : " + hopjl.getContainerId());
        }

        List<UpdatedContainerInfo> hopUpdatedContainerInfo = hopRMNodeNDBCompObject.getHopUpdatedContainerInfo();
        for (UpdatedContainerInfo hopuc : hopUpdatedContainerInfo) {
            LOG.debug("<HopUpdatedContainerInfo> <map> RMNodeid:" + hopuc.getRmnodeid() + "| ContainerID : " + hopuc.getUpdatedContainerInfoId());

        }

        List<ContainerId> hopContainerIdsToClean = hopRMNodeNDBCompObject.getHopContainerIdsToClean();

        for (ContainerId hopidclean : hopContainerIdsToClean) {
            LOG.debug("<HopContainerIdsToClean> <List> RMNodeid:" + hopidclean.getRmnodeid() + "| ContainerId : " + hopidclean.getContainerId());
        }

        List<FinishedApplications> hopFinishedApplications = hopRMNodeNDBCompObject.getHopFinishedApplications();
        for (FinishedApplications hopfinishedapp : hopFinishedApplications) {
            LOG.debug("<HopFinishedApplications> <List>  RMNodeID :" + hopfinishedapp.getRMNodeID() + "| ApplicationId : " + hopfinishedapp.getApplicationId());
        }

        List<ContainerStatus> hopContainersStatus = hopRMNodeNDBCompObject.getHopContainersStatus();
        for (ContainerStatus hopCS : hopContainersStatus) {
            LOG.debug("<HopContainersStatus> <map> HopContainerStatus rmnodeid  : " + hopCS.getRMNodeId() + "|Value : Container id : " + hopCS.getContainerid() + "| State : " + hopCS.getState());
        }
    }

    @Override
    public void run() {
        while (active) {
            try {
                //first ask him sleep

                RMNodeComps hopRMNodeCompObject = null;
                hopRMNodeCompObject = (RMNodeComps) NdbEventStreamingReceiver.blockingQueue.take();
                if (hopRMNodeCompObject != null) {
                    printHopsRMNodeComps(hopRMNodeCompObject);
                           
                 try {
                        rmNode = processHopRMNodeComps(hopRMNodeCompObject);
                        LOG.debug("HOP :: RMNodeWorker rmNode:" + rmNode);
                    } catch (IOException ex) {
                        LOG.error("HOP :: Error retrieving rmNode:" + ex, ex);
                    }

                    if (rmNode != null) {
                        updateRMContext(rmNode);
                        triggerEvent(rmNode, hopRMNodeCompObject.getPendingEvent());
                    }
                    
                }
            } catch (InterruptedException ex) {
                Logger.getLogger(NdbEventStreamingProcessor.class.getName()).log(Level.SEVERE, null, ex);
            }

        }

    }

}
