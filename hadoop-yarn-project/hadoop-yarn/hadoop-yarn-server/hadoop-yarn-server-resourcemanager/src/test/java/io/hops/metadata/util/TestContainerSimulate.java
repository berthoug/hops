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
package io.hops.metadata.util;

import io.hops.exception.StorageException;
import io.hops.exception.StorageInitializtionException;
import io.hops.metadata.yarn.dal.ContainerStatusDataAccess;
import io.hops.metadata.yarn.dal.ContainersLogsDataAccess;
import io.hops.metadata.yarn.dal.RMNodeDataAccess;
import io.hops.metadata.yarn.dal.YarnProjectsQuotaDataAccess;
import io.hops.metadata.yarn.dal.rmstatestore.ApplicationAttemptStateDataAccess;
import io.hops.metadata.yarn.dal.rmstatestore.ApplicationStateDataAccess;
import io.hops.metadata.yarn.dal.util.YARNOperationType;
import io.hops.metadata.yarn.entity.ContainersLogs;
import io.hops.metadata.yarn.entity.YarnProjectsQuota;
import io.hops.metadata.yarn.entity.rmstatestore.ApplicationAttemptState;
import io.hops.metadata.yarn.entity.rmstatestore.ApplicationState;
import io.hops.transaction.handler.LightWeightRequestHandler;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author rizvi
 */
public class TestContainerSimulate {
  private static final Log LOG = LogFactory.getLog(TestContainerSimulate.class);
  
    @Before
    public void setup() throws IOException {
            Configuration conf = new YarnConfiguration();
            LOG.info("DFS_STORAGE_DRIVER_JAR_FILE : " + YarnAPIStorageFactory.DFS_STORAGE_DRIVER_JAR_FILE);
            LOG.info("DFS_STORAGE_DRIVER_JAR_FILE_DEFAULT : " + YarnAPIStorageFactory.DFS_STORAGE_DRIVER_JAR_FILE_DEFAULT);
            LOG.info("DFS_STORAGE_DRIVER_CLASS : " + YarnAPIStorageFactory.DFS_STORAGE_DRIVER_CLASS);
            LOG.info("DFS_STORAGE_DRIVER_CLASS_DEFAULT : " + YarnAPIStorageFactory.DFS_STORAGE_DRIVER_CLASS_DEFAULT);
                    
            YarnAPIStorageFactory.setConfiguration(conf);
            RMStorageFactory.setConfiguration(conf);   
            
            //YarnAPIStorageFactory.setConfiguration(conf);
            //RMStorageFactory.setConfiguration(conf);
            RMUtilities.InitializeDB();
    }
    
    @Test
    public void LoadDBforSumulation() throws IOException {      
      try 
      {   
        long lStartTime = System.nanoTime();	
        
        /*final List<ContainersLogs> hopYarnContainersLogs = new ArrayList<ContainersLogs>();
        for (int i = 0 ; i< 10000; i++ )
          hopYarnContainersLogs.add(new ContainersLogs("container_" + i ,0,10,ContainerExitStatus.SUCCESS));*/
        int ROW_COUNT = 1000;
        final List<YarnProjectsQuota> hopYarnProjectsQuota = new ArrayList<YarnProjectsQuota>();
        for (int i = 0 ; i< ROW_COUNT; i++ )
          hopYarnProjectsQuota.add(new YarnProjectsQuota("Project_" + i,50,0));        
        
        LightWeightRequestHandler bomb;
        bomb = new LightWeightRequestHandler(YARNOperationType.TEST) {
            @Override
            public Object performTask() throws IOException {
                connector.beginTransaction();
                connector.writeLock(); 
                
                //ContainersLogsDataAccess<ContainersLogs> _clDA = (ContainersLogsDataAccess)RMStorageFactory.getDataAccess(ContainersLogsDataAccess.class);
                //_clDA.addAll(hopYarnContainersLogs);
                
                YarnProjectsQuotaDataAccess<YarnProjectsQuota> _pqDA = (YarnProjectsQuotaDataAccess)RMStorageFactory.getDataAccess(YarnProjectsQuotaDataAccess.class);
                _pqDA.addAll(hopYarnProjectsQuota);
                

                connector.commit();
                return null;
            }
        };
        bomb.handle();        
        long lEndTime = System.nanoTime();
        long difference = lEndTime - lStartTime;
      	LOG.info("Elapsed milliseconds: " + difference/1000000);        
      } catch (StorageInitializtionException ex) {
        LOG.error(ex);
      } catch (StorageException ex) {
        LOG.error(ex);
      }    
    }
    
    @Test
    public void LoadDBforSumulation2() throws IOException {      
      try 
      {   
        long lStartTime = System.nanoTime();    
    
        int ROW_COUNT = 100000; // Test with highest M to 100K 
        final List<YarnProjectsQuota> hopYarnProjectsQuota = new ArrayList<YarnProjectsQuota>();
        for (int j = 1; j < ROW_COUNT; j*=2 ){
          int rows_to_add = j - (j/2);          
          LOG.info("To insert in the DB: " + rows_to_add + " for " + j);          
          for (int i = rows_to_add ; i < j; i++ )
             hopYarnProjectsQuota.add(new YarnProjectsQuota("Project_" + i,50,0));
          
              LightWeightRequestHandler bomb;
              bomb = new LightWeightRequestHandler(YARNOperationType.TEST) {
                  @Override
                  public Object performTask() throws IOException {
                      connector.beginTransaction();
                      connector.writeLock();
                      
                      

                      //ContainersLogsDataAccess<ContainersLogs> _clDA = (ContainersLogsDataAccess)RMStorageFactory.getDataAccess(ContainersLogsDataAccess.class);
                      //_clDA.addAll(hopYarnContainersLogs);

                      YarnProjectsQuotaDataAccess<YarnProjectsQuota> _pqDA = (YarnProjectsQuotaDataAccess)RMStorageFactory.getDataAccess(YarnProjectsQuotaDataAccess.class);
                      _pqDA.addAll(hopYarnProjectsQuota);

                      connector.commit();
                      return null;
                  }
              };
              bomb.handle();  
              
              TestAllInCache2(j);
              //TestSingleFetch2(j);
          
          LOG.info("Records in the DB: " + j);          
          LOG.info("--------------------------- ");          
        }
        
        
        
        
        
        
        long lEndTime = System.nanoTime();
        long difference = lEndTime - lStartTime;
      	LOG.info("Elapsed milliseconds: " + difference/1000000);        
      } catch (StorageInitializtionException ex) {
        LOG.error(ex);
      } catch (StorageException ex) {
        LOG.error(ex);        
      }catch (Exception ex) {
        LOG.error(ex);
      }
        
    }
    
    
    @Test 
    public void TestAllInCache() throws IOException {      
      
      
      for (int j = 1; j <=1000; j*=2){
        int TO_BE_CHARGED_PROJECT_COUNT = j;        
        final List<YarnProjectsQuota> toBeChangedYarnProjectsQuotaList = new ArrayList<YarnProjectsQuota>();
        for (int i = 0 ; i< TO_BE_CHARGED_PROJECT_COUNT; i++ )
          toBeChangedYarnProjectsQuotaList.add(new YarnProjectsQuota("Project_" + i, 50, 1));        
          
      
        long lStartTime = System.nanoTime();	
        LightWeightRequestHandler bomb;      
        bomb = new LightWeightRequestHandler(YARNOperationType.TEST) {
            @Override
            public Object performTask() throws IOException {
              connector.beginTransaction();
              connector.writeLock(); 
                
              //Get Data  ** YarnProjectsQuota ** in memory as cache
              YarnProjectsQuotaDataAccess _pqDA = (YarnProjectsQuotaDataAccess)RMStorageFactory.getDataAccess(YarnProjectsQuotaDataAccess.class);
              Map<String , YarnProjectsQuota> hopYarnProjectsQuotaMap = _pqDA.getAll();
              
              //Charged projects map
              Map<String , YarnProjectsQuota> chargedYarnProjectsQuotaMap = new HashMap<String , YarnProjectsQuota>();
              
              //Calculate the charge and fill the  Charged-projects-map(chargedYarnProjectsQuotaMap)
              for(YarnProjectsQuota toBeChangedYarnProjectsQuota : toBeChangedYarnProjectsQuotaList){                
                YarnProjectsQuota _temp = hopYarnProjectsQuotaMap.get(toBeChangedYarnProjectsQuota.getProjectid());
                if (_temp != null){                  
                  chargedYarnProjectsQuotaMap.put(toBeChangedYarnProjectsQuota.getProjectid(),new YarnProjectsQuota(_temp.getProjectid(), 
                          _temp.getRemainingQuota() - toBeChangedYarnProjectsQuota.getTotalUsedQuota(), 
                          _temp.getTotalUsedQuota() + toBeChangedYarnProjectsQuota.getTotalUsedQuota()) );
                }
              }
              
              _pqDA.addAll(chargedYarnProjectsQuotaMap.values());             
              

              connector.commit();
              return null;
            }
        };
        bomb.handle();                  
        
        long lEndTime = System.nanoTime();
        long difference = lEndTime - lStartTime;
      	LOG.info("For " + j + "batch Elapsed milliseconds: " + difference/1000000);  
      }
    }
    
    @Test 
    public void TestSingleFetch() throws IOException {      
      
      
      for (int j = 1; j <=1000; j*=2){
        int TO_BE_CHARGED_PROJECT_COUNT = j;        
        final List<YarnProjectsQuota> toBeChangedYarnProjectsQuotaList = new ArrayList<YarnProjectsQuota>();
        for (int i = 0 ; i< TO_BE_CHARGED_PROJECT_COUNT; i++ )
          toBeChangedYarnProjectsQuotaList.add(new YarnProjectsQuota("Project_" + i, 50, 1));        
          
      
        long lStartTime = System.nanoTime();	
        LightWeightRequestHandler bomb;      
        bomb = new LightWeightRequestHandler(YARNOperationType.TEST) {
            @Override
            public Object performTask() throws IOException {
              connector.beginTransaction();
              connector.writeLock(); 
                
              //Get Data  ** YarnProjectsQuota ** in memory as cache
              YarnProjectsQuotaDataAccess _pqDA = (YarnProjectsQuotaDataAccess)RMStorageFactory.getDataAccess(YarnProjectsQuotaDataAccess.class);
              //Map<String , YarnProjectsQuota> hopYarnProjectsQuotaMap = _pqDA.getAll();
              
              //Charged projects map
              Map<String , YarnProjectsQuota> chargedYarnProjectsQuotaMap = new HashMap<String , YarnProjectsQuota>();
              
              //Calculate the charge and fill the  Charged-projects-map(chargedYarnProjectsQuotaMap)
              for(YarnProjectsQuota toBeChangedYarnProjectsQuota : toBeChangedYarnProjectsQuotaList){                
                YarnProjectsQuota _temp =(YarnProjectsQuota)_pqDA.findEntry(toBeChangedYarnProjectsQuota.getProjectid());
                if (_temp != null){                  
                  chargedYarnProjectsQuotaMap.put(toBeChangedYarnProjectsQuota.getProjectid(),new YarnProjectsQuota(_temp.getProjectid(), 
                          _temp.getRemainingQuota() - toBeChangedYarnProjectsQuota.getTotalUsedQuota(), 
                          _temp.getTotalUsedQuota() + toBeChangedYarnProjectsQuota.getTotalUsedQuota()) );
                }
              }
              
              _pqDA.addAll(chargedYarnProjectsQuotaMap.values());             
              

              connector.commit();
              return null;
            }
        };
        bomb.handle();                  
        
        long lEndTime = System.nanoTime();
        long difference = lEndTime - lStartTime;
      	LOG.info("For " + j + " batch Elapsed milliseconds: " + difference/1000000);  
      }
    }
    
    
    //@Test 
    public void TestAllInCache2(int rows_in_db) throws IOException {          
      
      
        int TO_BE_CHARGED_PROJECT_COUNT = 100;        
        final List<YarnProjectsQuota> toBeChangedYarnProjectsQuotaList = new ArrayList<YarnProjectsQuota>();
        for (int i = 0 ; i< TO_BE_CHARGED_PROJECT_COUNT; i++ )
          toBeChangedYarnProjectsQuotaList.add(new YarnProjectsQuota("Project_" + i, 50, 1));        
          
      
        long lStartTime = System.nanoTime();	
        LightWeightRequestHandler bomb;      
        bomb = new LightWeightRequestHandler(YARNOperationType.TEST) {
            @Override
            public Object performTask() throws IOException {
              connector.beginTransaction();
              connector.writeLock(); 
                
              //Get Data  ** YarnProjectsQuota ** in memory as cache
              YarnProjectsQuotaDataAccess _pqDA = (YarnProjectsQuotaDataAccess)RMStorageFactory.getDataAccess(YarnProjectsQuotaDataAccess.class);
              Map<String , YarnProjectsQuota> hopYarnProjectsQuotaMap = _pqDA.getAll();
              
              //Charged projects map
              Map<String , YarnProjectsQuota> chargedYarnProjectsQuotaMap = new HashMap<String , YarnProjectsQuota>();
              
              //Calculate the charge and fill the  Charged-projects-map(chargedYarnProjectsQuotaMap)
              for(YarnProjectsQuota toBeChangedYarnProjectsQuota : toBeChangedYarnProjectsQuotaList){                
                YarnProjectsQuota _temp = hopYarnProjectsQuotaMap.get(toBeChangedYarnProjectsQuota.getProjectid());
                if (_temp != null){                  
                  chargedYarnProjectsQuotaMap.put(toBeChangedYarnProjectsQuota.getProjectid(),new YarnProjectsQuota(_temp.getProjectid(), 
                          _temp.getRemainingQuota() - toBeChangedYarnProjectsQuota.getTotalUsedQuota(), 
                          _temp.getTotalUsedQuota() + toBeChangedYarnProjectsQuota.getTotalUsedQuota()) );
                }
              }
              
              _pqDA.addAll(chargedYarnProjectsQuotaMap.values());             
              

              connector.commit();
              return null;
            }
        };
        bomb.handle();                  
        
        long lEndTime = System.nanoTime();
        long difference = lEndTime - lStartTime;
        
      	LOG.info("For AllinCache mode "+ rows_in_db+" rows Elapsed milliseconds: " + difference/1000000);  
      
    }
    
    
    //@Test 
    public void TestSingleFetch2(int rows_in_db) throws IOException {      
      
      
      //for (int j = 1; j <=1000; j*=2){
        int TO_BE_CHARGED_PROJECT_COUNT = 100;        
        final List<YarnProjectsQuota> toBeChangedYarnProjectsQuotaList = new ArrayList<YarnProjectsQuota>();
        for (int i = 0 ; i< TO_BE_CHARGED_PROJECT_COUNT; i++ )
          toBeChangedYarnProjectsQuotaList.add(new YarnProjectsQuota("Project_" + i, 50, 1));        
          
      
        long lStartTime = System.nanoTime();	
        LightWeightRequestHandler bomb;      
        bomb = new LightWeightRequestHandler(YARNOperationType.TEST) {
            @Override
            public Object performTask() throws IOException {
              connector.beginTransaction();
              connector.writeLock(); 
                
              //Get Data  ** YarnProjectsQuota ** in memory as cache
              YarnProjectsQuotaDataAccess _pqDA = (YarnProjectsQuotaDataAccess)RMStorageFactory.getDataAccess(YarnProjectsQuotaDataAccess.class);
              //Map<String , YarnProjectsQuota> hopYarnProjectsQuotaMap = _pqDA.getAll();
              
              //Charged projects map
              Map<String , YarnProjectsQuota> chargedYarnProjectsQuotaMap = new HashMap<String , YarnProjectsQuota>();
              
              //Calculate the charge and fill the  Charged-projects-map(chargedYarnProjectsQuotaMap)
              for(YarnProjectsQuota toBeChangedYarnProjectsQuota : toBeChangedYarnProjectsQuotaList){                
                YarnProjectsQuota _temp =(YarnProjectsQuota)_pqDA.findEntry(toBeChangedYarnProjectsQuota.getProjectid());
                if (_temp != null){                  
                  chargedYarnProjectsQuotaMap.put(toBeChangedYarnProjectsQuota.getProjectid(),new YarnProjectsQuota(_temp.getProjectid(), 
                          _temp.getRemainingQuota() - toBeChangedYarnProjectsQuota.getTotalUsedQuota(), 
                          _temp.getTotalUsedQuota() + toBeChangedYarnProjectsQuota.getTotalUsedQuota()) );
                }
              }
              
              _pqDA.addAll(chargedYarnProjectsQuotaMap.values());             
              

              connector.commit();
              return null;
            }
        };
        bomb.handle();                  
        
        long lEndTime = System.nanoTime();
        long difference = lEndTime - lStartTime;
      	LOG.info("For SingleFetch mode " + rows_in_db + " Elapsed milliseconds: " + difference/1000000);  
      //}
    }
    
}
