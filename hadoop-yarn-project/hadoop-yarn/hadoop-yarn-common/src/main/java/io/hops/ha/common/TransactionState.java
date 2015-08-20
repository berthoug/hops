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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.hadoop.yarn.api.records.ApplicationId;

public abstract class TransactionState {

  
  //TODO: Should we persist this id when the RT crashes and the NM starts 
  //sending HBs to the new RT?
  protected static AtomicInteger pendingEventId = new AtomicInteger(0);

  public enum TransactionType {

    RM,
    APP,
    NODE,
    INIT
  }

  private static final Log LOG = LogFactory.getLog(TransactionState.class);
  private AtomicInteger counter = new AtomicInteger(0);
  protected final Set<ApplicationId> appIds = new ConcurrentSkipListSet<ApplicationId>();
//  private final Lock counterLock = new ReentrantLock(true);
  private Set<Integer> rpcIds = new ConcurrentSkipListSet<Integer>();
  private int id=-1;
  private final boolean batch;
  
  public TransactionState(ApplicationId appId, int initialCounter, boolean batch) {
    if(appId!=null){
      addAppId(appId);
    }
    counter = new AtomicInteger(initialCounter);
    this.batch = batch;
  }

  public int getId(){
    return id;
  }
    public Set<ApplicationId> getAppIds(){
    return appIds;
  }
    
  abstract void addAppId(ApplicationId appId);
    
  public synchronized void incCounter(Enum type) {
    counter.incrementAndGet();
  }

  public synchronized void decCounter(Enum type) throws IOException {
    int value = counter.decrementAndGet();
    if(!batch && value==0){
      commit(true);
    }
  }

  public int getCounter(){
      return counter.get();
  }
 
  public void addRPCId(int rpcId){
    if(rpcId>0 && id<0){
      id = rpcId;
    }
    rpcIds.add(rpcId);
  }
  
  public Set<Integer> getRPCIds(){
    return rpcIds;
  }

  public abstract void commit(boolean first) throws IOException;
}
