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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

public class TransactionStateManager implements Runnable{
  private static final Log LOG = LogFactory.getLog(TransactionStateManager.class);
  TransactionState currentTransactionState;
  Lock lock = new ReentrantLock(true);
  AtomicInteger acceptedRPC =new AtomicInteger();
  List<transactionStateWrapper> curentRPCs = new CopyOnWriteArrayList<transactionStateWrapper>();
  int batchMaxSize = 50;
  int batchMaxDuration = 100;
  
  public TransactionStateManager(Configuration conf){
    currentTransactionState = new TransactionStateImpl(
                TransactionState.TransactionType.RM, 0, true);
    batchMaxSize = conf.getInt(YarnConfiguration.HOPS_BATCH_MAX_SIZE, YarnConfiguration.DEFAULT_HOPS_BATCH_MAX_SIZE);
    batchMaxDuration = conf.getInt(YarnConfiguration.HOPS_BATCH_MAX_DURATION, YarnConfiguration.DEFAULT_HOPS_BATCH_MAX_DURATION);
  }
  
  @Override
  public void run() {
    lock.lock();
    long commitDuration = 0;
    long startTime = System.currentTimeMillis();
    double accumulatedCycleDuration=0;
    long nbCycles =0;
    List<Long> duration = new ArrayList<Long>();
    List<Integer> rpcCount = new ArrayList<Integer>();
    double accumulatedRPCCount = 0;
    long t1=0;
    long t2=0;
    long t3=0;
    long t4=0;
    while(true){
      try {
        //create new transactionState
        currentTransactionState = new TransactionStateImpl(
                TransactionState.TransactionType.RM, 0, true);
        curentRPCs = new CopyOnWriteArrayList<transactionStateWrapper>();
        acceptedRPC.set(0);
        //accept RPCs
        lock.unlock();
        commitDuration = System.currentTimeMillis()-startTime;
        t3= commitDuration;
//        Thread.sleep(Math.max(0, 10-commitDuration));
        waitForBatch(Math.max(0, batchMaxDuration-commitDuration));
        t4= System.currentTimeMillis() - startTime;
        //stop acception RPCs
        lock.lock();
        
        long cycleDuration = System.currentTimeMillis() - startTime;
        if(cycleDuration> batchMaxDuration + 10){
          LOG.error("Cycle too long: " + cycleDuration + "| " + t1 + ", " + t2 + ", " + t3 + ", " + t4);
        }
        nbCycles++;
        accumulatedCycleDuration+=cycleDuration;
        duration.add(cycleDuration);
        rpcCount.add(acceptedRPC.get());
        accumulatedRPCCount+=acceptedRPC.get();
        if(duration.size()>39){
          double avgCycleDuration=accumulatedCycleDuration/nbCycles;
          LOG.info("cycle duration: " + avgCycleDuration + " " + duration.toString());
          double avgRPCCount = accumulatedRPCCount/nbCycles;
          LOG.info("rpc count: " + avgRPCCount + " " + rpcCount.toString());
          duration = new ArrayList<Long>();
          rpcCount = new ArrayList<Integer>();
        }
        
        startTime = System.currentTimeMillis();
        //wait for all the accepted RPCs to finish
        int count = 0;
        long startWaiting = System.currentTimeMillis();
        while(currentTransactionState.getCounter() != 0){
          if(System.currentTimeMillis()-startWaiting>1000){
            startWaiting = System.currentTimeMillis();
            count++;
            LOG.error("waiting too long " + count + " counter: " + currentTransactionState.getCounter());
            for(transactionStateWrapper w : curentRPCs){
              if(w.getRPCCounter()>0){
                LOG.error("rpc not finishing: " + w.getRPCID() + " type: " + w.getRPCType() + ", counter: " + w.getRPCCounter() + " running events: " + w.getRunningEvents());
              }
            }
          }
        }
        
        t1= System.currentTimeMillis() - startTime;
        //commit the transactionState
        currentTransactionState.commit(true);
        t2= System.currentTimeMillis() - startTime;
      } catch (IOException ex) {
        Logger.getLogger(TransactionStateManager.class.getName()).
                log(Level.SEVERE, null, ex);
      } catch (InterruptedException ex) {
        Logger.getLogger(TransactionStateManager.class.getName()).
                log(Level.SEVERE, null, ex);
      }
    }
  }
  
  private void waitForBatch(long maxTime) throws InterruptedException {
    long start = System.currentTimeMillis();
    while (true) {
      if (System.currentTimeMillis() - start > maxTime || acceptedRPC.get() >= batchMaxSize) {
        break;
      }
      Thread.sleep(1);
    }
  }
  
  public TransactionState getCurrentTransactionState(int rpcId, String callingFuncition) {
    while (true) {
      if (acceptedRPC.incrementAndGet() < batchMaxSize) {
      acceptedRPC.incrementAndGet();
              lock.lock();
        try {
          transactionStateWrapper wrapper = new transactionStateWrapper((TransactionStateImpl)currentTransactionState,
                  TransactionState.TransactionType.RM, rpcId, callingFuncition);
          wrapper.incCounter(TransactionState.TransactionType.INIT);
          wrapper.addRPCId(rpcId);
          curentRPCs.add(wrapper);
          return wrapper;
        } finally {
          lock.unlock();
        }
      } else {
        acceptedRPC.decrementAndGet();
        try{
          Thread.sleep(1);
        }catch(InterruptedException e){
          LOG.warn(e, e);
        }
      }
    }
  }
  
  public void start(){
    new Thread(this).start();
  }
}
