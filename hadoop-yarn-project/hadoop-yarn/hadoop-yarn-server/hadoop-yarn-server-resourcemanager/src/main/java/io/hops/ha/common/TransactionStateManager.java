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
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class TransactionStateManager implements Runnable{
  private static final Log LOG = LogFactory.getLog(TransactionStateManager.class);
  TransactionState currentTransactionState;
  Lock lock = new ReentrantLock(true);
  int acceptedRPC =0;
  
  public TransactionStateManager(){
    currentTransactionState = new TransactionStateImpl(
                TransactionState.TransactionType.RM, 0, true);
  }
  
  @Override
  public void run() {
    lock.lock();
    long commitDuration = 0;
    long startTime = System.currentTimeMillis();
    double accumulatedCycleDuration=0;
    long nbCycles =0;
    List<Long> duration = new ArrayList<Long>();
    while(true){
      try {
        //create new transactionState
        currentTransactionState = new TransactionStateImpl(
                TransactionState.TransactionType.RM, 0, true);
        acceptedRPC=0;
        //accept RPCs
        lock.unlock();
        commitDuration = System.currentTimeMillis()-startTime;
//        Thread.sleep(Math.max(0, 10-commitDuration));
          waitForBatch(Math.max(0, 10-commitDuration));
        //stop acception RPCs
        lock.lock();
        
        long cycleDuration = System.currentTimeMillis() - startTime;
        nbCycles++;
        accumulatedCycleDuration+=cycleDuration;
        duration.add(cycleDuration);
        if(duration.size()>39){
          double avgCycleDuration=accumulatedCycleDuration/nbCycles;
          LOG.info("cycle duration: " + cycleDuration + "(" + avgCycleDuration + ") " + duration.toString());
          duration = new ArrayList<Long>();
        }
        
        startTime = System.currentTimeMillis();
        //wait for all the accepted RPCs to finish
        while(currentTransactionState.getCounter() != 0){
        }
        //commit the transactionState
        currentTransactionState.commit(true);
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
//    long start = System.currentTimeMillis();
//    while (true) {
//      if (System.currentTimeMillis() - start > maxTime /*|| acceptedRPC >= 1000*/) {
//        break;
//      }
//      Thread.sleep(1);
//    }
    Thread.sleep(maxTime);
  }
  
  public TransactionState getCurrentTransactionState(int rpcId, String callingFuncition) {
//    while (true) {
      lock.lock();
//      if (acceptedRPC < 1000) {
        try {
//          currentTransactionState.incCounter(
//                  TransactionState.TransactionType.INIT);
          transactionStateWrapper wrapper = new transactionStateWrapper((TransactionStateImpl)currentTransactionState,
                  TransactionState.TransactionType.RM);
          wrapper.addRPCId(rpcId, callingFuncition);
          wrapper.incCounter(TransactionState.TransactionType.INIT);
          return wrapper;
        } finally {
          lock.unlock();
        }
//      } else {
//        lock.unlock();
//      }
//    }
  }
  
  public void start(){
    new Thread(this).start();
  }
}
