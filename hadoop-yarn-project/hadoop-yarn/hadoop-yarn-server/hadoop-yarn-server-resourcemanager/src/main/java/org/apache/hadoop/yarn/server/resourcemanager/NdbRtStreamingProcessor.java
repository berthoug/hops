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
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.util.ConverterUtils;

/**
 *
 * @author sri
 */
public class NdbRtStreamingProcessor implements Runnable {

  private static final Log LOG = LogFactory.getLog(
          NdbRtStreamingProcessor.class);
  private boolean running = false;
  private final RMContext context;
  private RMNode rmNode;
  private int receivehbcounter=0;
  public NdbRtStreamingProcessor(RMContext context) {
    this.context = context;
  }

  public void printStreamingRTComps(StreamingRTComps streamingRTComps) {
//    List<ApplicationId> applicationIdList = streamingRTComps.getFinishedApp();
//    for (ApplicationId appId : applicationIdList) {
//      System.out.println("<Processor> Finished application : appid : " + appId.toString());
//      LOG.info("<Processor> Finished application : appid : " + appId.toString());
//    }
//
//    Set<ContainerId> containerIdList = streamingRTComps.getContainersToClean();
//    for (ContainerId conId : containerIdList) {
//      System.out.println("<Processor> Containers to clean  containerid: " + conId.toString());
//      LOG.info("<Processor> Containers to clean  containerid: " + conId.toString());
//    }
      ++receivehbcounter;
    //System.out.println("<Processor> RMnode id : " + streamingRTComps.getNodeId() + " next heartbeat : " + streamingRTComps.isNextHeartbeat());
    LOG.info("RTReceived: " + streamingRTComps.getNodeId() + " nexthb: "+streamingRTComps.isNextHeartbeat() +" counter: "+receivehbcounter );

  }

  @Override
  public void run() {
    running = true;
    while (running) {
      if (!context.getRMGroupMembershipService().isLeader()) {
        try {

          StreamingRTComps streamingRTComps = null;
          streamingRTComps = (StreamingRTComps) NdbRtStreamingReceiver.blockingRTQueue.take();
          if (streamingRTComps != null) {
           // printStreamingRTComps(streamingRTComps);

            NodeId nodeId = ConverterUtils.toNodeId(streamingRTComps.getNodeId());
            rmNode = context.getActiveRMNodes().get(nodeId);
            //LOG.debug("HOP :: RTStreaming processor rmNode:" + rmNode);
            if (rmNode != null) {
              rmNode.setContainersToCleanUp(streamingRTComps.getContainersToClean());
              rmNode.setAppsToCleanup(streamingRTComps.getFinishedApp());
              rmNode.setNextHeartBeat(streamingRTComps.isNextHeartbeat());
            }
          }
        } catch (InterruptedException ex) {
          LOG.error(ex, ex);
        }

      }
    }

  }

  public void stop() {
    running = false;
  }

}
