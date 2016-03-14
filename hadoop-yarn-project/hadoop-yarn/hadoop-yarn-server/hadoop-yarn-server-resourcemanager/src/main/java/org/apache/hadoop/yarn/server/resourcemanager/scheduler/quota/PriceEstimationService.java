/*
 * Copyright 2016 Apache Software Foundation.
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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.quota;

import io.hops.metadata.util.RMStorageFactory;
import io.hops.metadata.yarn.dal.YarnHistoryPriceDataAccess;
import io.hops.metadata.yarn.dal.YarnRunningPriceDataAccess;
import io.hops.metadata.yarn.dal.util.YARNOperationType;
import io.hops.metadata.yarn.entity.YarnHistoryPrice;
import io.hops.metadata.yarn.entity.YarnRunningPrice;
import io.hops.transaction.handler.LightWeightRequestHandler;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.ContainersLogsService;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;

/**
 * @author rizvi
 */
public class PriceEstimationService  extends AbstractService {
  
  private Configuration conf;
  private RMContext rmcontext;
  private volatile boolean stopped = false;
  private Thread priceCalculationThread;
  private float tippingPointMBinPercentage;
  private float tippingPointVCinPercentage;
  private float increnetFactorForMemory;
  private float increnetFactorForVirtualCore;
  private float minimumPricePerTickMB;
  private float minimumPricePerTickVC;
  private int priceCalculationIntervel;
  private float latestPrice;
  private long latestPriceTick;
  
  private YarnRunningPriceDataAccess runningPriceDA;
  private YarnHistoryPriceDataAccess historyPriceDA;
  
  private static final Log LOG = LogFactory.getLog(PriceEstimationService.class);

  public PriceEstimationService(RMContext rmctx) {
    super("Price estimation service");    
    rmcontext =  rmctx;    
  }
  
   @Override
  public void serviceInit(Configuration conf) throws Exception {
    LOG.info("Initializing price estimation service");
    this.conf = conf;
    
    // Initialize config parameters
    this.tippingPointMBinPercentage = this.conf.getFloat(YarnConfiguration.MAXIMUM_PERCENTAGE_OF_MEMORY_USAGE_WITH_MINIMUM_PRICE,YarnConfiguration.DEFAULT_MAXIMUM_PERCENTAGE_OF_MEMORY_USAGE_WITH_MINIMUM_PRICE);
    this.tippingPointVCinPercentage = this.conf.getFloat(YarnConfiguration.MAXIMUM_PERCENTAGE_OF_VIRTUAL_CORE_USAGE_WITH_MINIMUM_PRICE,YarnConfiguration.DEFAULT_MAXIMUM_PERCENTAGE_OF_VIRTUAL_CORE_USAGE_WITH_MINIMUM_PRICE);
    this.increnetFactorForMemory = 10;
    this.increnetFactorForVirtualCore = 10;
    this.minimumPricePerTickMB = this.conf.getFloat(YarnConfiguration.MINIMUM_PRICE_PER_TICK_FOR_MEMORY,YarnConfiguration.DEFAULT_MINIMUM_PRICE_PER_TICK_FOR_VIRTUAL_CORE);
    this.minimumPricePerTickVC = this.conf.getFloat(YarnConfiguration.MINIMUM_PRICE_PER_TICK_FOR_MEMORY,YarnConfiguration.DEFAULT_MINIMUM_PRICE_PER_TICK_FOR_VIRTUAL_CORE);
    this.priceCalculationIntervel = this.conf.getInt(YarnConfiguration.QUOTAS_CONTAINERS_LOGS_MONITOR_INTERVAL,YarnConfiguration.DEFAULT_QUOTAS_CONTAINERS_LOGS_MONITOR_INTERVAL);;
    
    // Initialize DataAccesses
    this.runningPriceDA = (YarnRunningPriceDataAccess)RMStorageFactory.getDataAccess(YarnRunningPriceDataAccess.class);
    this.historyPriceDA = (YarnHistoryPriceDataAccess)RMStorageFactory.getDataAccess(YarnHistoryPriceDataAccess.class);
                    
  }
  
  @Override
  protected void serviceStart() throws Exception {
    assert !stopped : "starting when already stopped";
    LOG.info("Starting a new price estimation service.");
    
    priceCalculationThread = new Thread(new PriceEstimationService.WorkingThread());
    priceCalculationThread.setName("Price estimation service");
    priceCalculationThread.start();
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    stopped = true;
    if (priceCalculationThread != null) {
      priceCalculationThread.interrupt();
    }
    super.serviceStop();
    LOG.info("Stopping the price estimation service.");
  }

  
  private class WorkingThread implements Runnable  {

    @Override
    public void run() {
      LOG.info("Price estimation service started");
      long iteration = 0;
      while (!stopped && !Thread.currentThread().isInterrupted()) {
        try {
         
          // Calculate the price based on resource usage
          CalculateNewPrice();
          
          // Pass the latest price to the Containers Log Service          
          if(rmcontext.isLeadingRT()) {            
            ContainersLogsService cl = rmcontext.getContainersLogsService();
            if (cl!=null)
            {
              cl.insertPriceEvent(latestPrice, latestPriceTick);
            }
            else
            {
              LOG.debug("RIZ: ContainersLogsService not initialized!");
            }
          }
          
          LOG.debug("RIZ: working thread iteration " + iteration++);
          
          Thread.sleep(priceCalculationIntervel);
        } catch (IOException ex) {
          LOG.error(ex, ex);
        } catch (InterruptedException ex) {
          Logger.getLogger(PriceEstimationService.class.getName()).
                  log(Level.SEVERE, null, ex);
        }
          
      }
      LOG.info("Quota scheduler thread is exiting gracefully");
    }
  }

  protected void CalculateNewPrice() throws IOException {
    
    try {
            LightWeightRequestHandler prepareHandler;
            prepareHandler = new LightWeightRequestHandler(YARNOperationType.TEST) {              
                @Override
                public Object performTask() throws IOException {
                    connector.beginTransaction();
                    connector.writeLock();                                                
                    
                    if ( runningPriceDA != null && historyPriceDA !=null){                                            
                        
                          long _time = System.currentTimeMillis();
                          QueueMetrics metrics = rmcontext.getScheduler().getRootQueueMetrics();
                         
                          int totalMB = metrics.getAllocatedMB() + metrics.getAvailableMB();
                          int totalCore = metrics.getAllocatedVirtualCores() + metrics.getAvailableVirtualCores();
                          
                          int toUseMB = metrics.getAllocatedMB() + metrics.getPendingMB();                          
                          float tippingPointMB= tippingPointMBinPercentage; // After 80% the price should be increasing
                          float incrementBaseMB = ((float)toUseMB /(float)totalMB) - tippingPointMB;   
                          incrementBaseMB = incrementBaseMB > 0  ? incrementBaseMB : 0;
                          float incrementFactorMB = increnetFactorForMemory; // The factor the value will be incremented
                          float newPriceMB = minimumPricePerTickMB + (incrementBaseMB * incrementFactorMB);
                          LOG.info("MB use: "+ toUseMB+ " of " + totalMB + " ("+ (int)((float)toUseMB * 100 /(float)totalMB) +"%) " + incrementBaseMB + "% over limit");
                          LOG.info("MB price: " + newPriceMB);

                          int toUseCore = metrics.getAllocatedVirtualCores() + metrics.getPendingVirtualCores();
                          float tippingPointVC= tippingPointVCinPercentage; // After 80% the price should be increasing
                          float incrementBaseVC = ((float)toUseCore /(float)totalCore) - tippingPointVC;   
                          incrementBaseVC = incrementBaseVC > 0  ? incrementBaseVC : 0;
                          float incrementFactorVC = increnetFactorForVirtualCore; // The factor the value will be incremented
                          float newPriceVC = minimumPricePerTickVC + (incrementBaseVC * incrementFactorVC);
                          LOG.info("VC use: "+ toUseMB+ " of " + totalMB + " ("+(int)((float)toUseCore * 100 /(float)totalCore)+"%) " + incrementBaseVC+ "% over limit");
                          LOG.info("VC price: " + newPriceVC);                         
                          
                          latestPrice = newPriceMB + newPriceVC;
                          latestPriceTick = _time;
                         
                          runningPriceDA.add(new YarnRunningPrice(1, latestPriceTick, latestPrice));
                          historyPriceDA.add(new YarnHistoryPrice(latestPriceTick, latestPrice));
                         
                          
                          LOG.info("New price: " + latestPrice + " (" + latestPriceTick + ")");
                          //LOG.info("RIZ: Size for YarnRunningPrice :" + _rpDA.getAll().size() + " ");
                        
                    }else
                    {
                        LOG.info("DataAccess failed!");

                    }                    
                    connector.commit();
                    return null;
                }
            };
            prepareHandler.handle();            
            
        } catch (Exception e) {
            LOG.error(e);
        }
  }
  
  
}
