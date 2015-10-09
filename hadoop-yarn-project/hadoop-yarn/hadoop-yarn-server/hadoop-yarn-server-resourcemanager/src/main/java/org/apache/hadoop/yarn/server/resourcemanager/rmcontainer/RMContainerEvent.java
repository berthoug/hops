/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.rmcontainer;

import io.hops.ha.common.TransactionState;
import java.util.Queue;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.event.AbstractEventTransaction;

public class RMContainerEvent
    extends AbstractEventTransaction<RMContainerEventType> {

  private final ContainerId containerId;
  private final Queue<Long> times;
  
  public RMContainerEvent(ContainerId containerId, RMContainerEventType type,
      TransactionState transactionState) {
    super(type, transactionState);
    this.containerId = containerId;
    times=null;
  }

  public RMContainerEvent(ContainerId containerId, RMContainerEventType type,
      TransactionState transactionState, Queue<Long> times) {
    super(type, transactionState);
    this.containerId = containerId;
    this.times = times;
  }
  
  public ContainerId getContainerId() {
    return this.containerId;
  }
  
  public Queue<Long>getTimes(){
    return times;
  }
}
