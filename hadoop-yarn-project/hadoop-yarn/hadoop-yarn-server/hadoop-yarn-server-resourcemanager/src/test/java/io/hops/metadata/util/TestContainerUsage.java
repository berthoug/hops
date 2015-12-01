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
package io.hops.metadata.util;

import io.hops.DalDriver;
import io.hops.StorageConnector;
import io.hops.exception.StorageException;
import io.hops.exception.StorageInitializtionException;
import io.hops.metadata.yarn.TablesDef;
import io.hops.metadata.yarn.dal.ContainerStatusDataAccess;
import io.hops.metadata.yarn.dal.YarnContainersLogsDataAccess;
import io.hops.metadata.yarn.dal.YarnProjectsQuotaDataAccess;
import io.hops.metadata.yarn.dal.rmstatestore.ApplicationStateDataAccess;
import io.hops.metadata.yarn.dal.util.YARNOperationType;
import io.hops.metadata.yarn.entity.ContainerStatus;
import io.hops.metadata.yarn.entity.YarnContainersLogs;
import io.hops.metadata.yarn.entity.YarnProjectsQuota;
import io.hops.metadata.yarn.entity.rmstatestore.ApplicationState;
import io.hops.transaction.handler.LightWeightRequestHandler;
import io.hops.transaction.handler.RequestHandler;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.TestResourceManager;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author rizvi
 */
public class TestContainerUsage {
   
    private static final Log LOG = LogFactory.getLog(TestContainerUsage.class);
    
    private final int DEFAULT_PENDIND_ID = 0;
    private StorageConnector connector = null;
    //RMStorageFactory storageFactory = new RMStorageFactory();
    
    @Before
    public void setup() throws IOException {
            Configuration conf = new YarnConfiguration();
            LOG.info("DFS_STORAGE_DRIVER_JAR_FILE : " + YarnAPIStorageFactory.DFS_STORAGE_DRIVER_JAR_FILE);
            LOG.info("DFS_STORAGE_DRIVER_JAR_FILE_DEFAULT : " + YarnAPIStorageFactory.DFS_STORAGE_DRIVER_JAR_FILE_DEFAULT);
            LOG.info("DFS_STORAGE_DRIVER_CLASS : " + YarnAPIStorageFactory.DFS_STORAGE_DRIVER_CLASS);
            LOG.info("DFS_STORAGE_DRIVER_CLASS_DEFAULT : " + YarnAPIStorageFactory.DFS_STORAGE_DRIVER_CLASS_DEFAULT);
                    
            //YarnAPIStorageFactory.setConfiguration(conf);
            RMStorageFactory.setConfiguration(conf);
            
            //RMUtilities.InitializeDB();
            
            this.connector =  RMStorageFactory.getConnector();
            RequestHandler.setStorageConnector(this.connector);           
            
    }
  
  
    //@Test
    public void TestContainerStatus() throws StorageException, IOException{
        LOG.info("--- START: TestContainerUsage ---");
        LOG.info("--- Checking ContainerStatus ---");

        try {           
            final List<ContainerStatus> hopContainersStatus = new ArrayList<ContainerStatus>();
            hopContainersStatus.add(new ContainerStatus("container7",TablesDef.ContainerStatusTableDef.STATE_RUNNING,"cont.. from riz", 0, "70", DEFAULT_PENDIND_ID));
            hopContainersStatus.add(new ContainerStatus("container8",TablesDef.ContainerStatusTableDef.STATE_RUNNING,"cont.. from riz", 0, "70", DEFAULT_PENDIND_ID));

            
            
            LightWeightRequestHandler bomb = new LightWeightRequestHandler(
              YARNOperationType.TEST) {
                @Override
                public Object performTask() throws IOException {
                    connector.beginTransaction();
                    connector.writeLock();
                    
                  
                    ContainerStatusDataAccess _csDA = (ContainerStatusDataAccess)RMStorageFactory.getDataAccess(ContainerStatusDataAccess.class);
                    _csDA.addAll(hopContainersStatus);
             
                    connector.commit();
                    return null;
                }
              };
            bomb.handle();
            
            
        } catch (StorageInitializtionException ex) {
          //LOG.error(ex);
        } catch (StorageException ex) {
          //LOG.error(ex);
        }       
    }
    
    
    @Test
    public void TestYarnContainersLogs() throws StorageException, IOException{
        LOG.info("--- START: TestContainerUsage ---");
        LOG.info("--- Checking YarnContainersLogs ---");

        try {           
            
            //hopContainersStatus.add(new ContainerStatus("container7",TablesDef.ContainerStatusTableDef.STATE_RUNNING,"cont.. from riz", 0, "70", DEFAULT_PENDIND_ID));
            //hopContainersStatus.add(new ContainerStatus("container8",TablesDef.ContainerStatusTableDef.STATE_RUNNING,"cont.. from riz", 0, "70", DEFAULT_PENDIND_ID));

            
            
            LightWeightRequestHandler bomb = new LightWeightRequestHandler(
              YARNOperationType.TEST) {
                @Override
                public Object performTask() throws IOException {
                    connector.beginTransaction();
                    connector.readLock();
                    
                    //Get Data  ** YarnContainersLogs **
                    YarnContainersLogsDataAccess _csDA = (YarnContainersLogsDataAccess)RMStorageFactory.getDataAccess(YarnContainersLogsDataAccess.class);
                    Map<String , YarnContainersLogs> hopYarnContainersLogs = _csDA.getAll();
                    
                    List<YarnProjectsQuota> chargedYarnProjectsQuota = new ArrayList<YarnProjectsQuota>();
                    List<YarnContainersLogs> toBeRemovedYarnContainersLogs = new ArrayList<YarnContainersLogs>();
                    
                    // Calculate the quota 
                    LOG.info("RIZ:: YarnContainersLogs count : " + hopYarnContainersLogs.size());                    
                    for(  Map.Entry<String , YarnContainersLogs> _ycl : hopYarnContainersLogs.entrySet()){
                        
                        LOG.info("RIZ:: YarnContainersLogs entry : " + _ycl.getValue().toString());
                        ContainerId _cId = ConverterUtils.toContainerId(_ycl.getValue().getContainerid());
                        ApplicationId _appId = _cId.getApplicationAttemptId().getApplicationId();                        
                        
                        //Get Data from ** ApplicationState ** by ApplicationId
                        ApplicationStateDataAccess _appStatDS = (ApplicationStateDataAccess)RMStorageFactory.getDataAccess(ApplicationStateDataAccess.class);
                        ApplicationState _appStat =(ApplicationState)_appStatDS.findByApplicationId(_appId.toString());
                        LOG.info("RIZ:: App : " + _appId.toString() + " User : " + _appStat.getUser()); 
                        
                        // Calculate the charge
                        String _projectid = _appStat.getUser().split("__")[0];
                        int _charge = _ycl.getValue().getStop() -_ycl.getValue().getStart();                      
                        
                        // Prepare lists
                        chargedYarnProjectsQuota.add(new YarnProjectsQuota(_projectid, _charge));   
                        toBeRemovedYarnContainersLogs.add((YarnContainersLogs)_ycl.getValue());
                    }
                    
                    // Deleta the ** YarnContainersLogs **
                    _csDA.removeAll(toBeRemovedYarnContainersLogs);
                    
                    
                    // Show all charged project
                    for(YarnProjectsQuota _cpq : chargedYarnProjectsQuota){
                        
                        LOG.info("RIZ:: Charged projects: " + _cpq.toString() + " charge amount:" + _cpq.getCredit());
                    }
                    
                    
                    
                    // YarnProjectsQuota
                    YarnProjectsQuotaDataAccess _pjDA = (YarnProjectsQuotaDataAccess)RMStorageFactory.getDataAccess(YarnProjectsQuotaDataAccess.class);
                    //List<YarnProjectsQuota> _listpq = new ArrayList<YarnProjectsQuota>();
                    //_listpq.add( new YarnProjectsQuota("Project-13", 90));                    
                    //_pjDA.addAll(_listpq);
                    
                    List<YarnProjectsQuota> deductedYarnProjectsQuota = new ArrayList<YarnProjectsQuota>();                    
                    Map<String , YarnProjectsQuota> hopYarnProjectsQuota = _pjDA.getAll();                    
                    LOG.info("RIZ:: YarnProjectsQuota count : " + hopYarnProjectsQuota.size());
                    for(Map.Entry<String , YarnProjectsQuota> _yps : hopYarnProjectsQuota.entrySet()){
                        LOG.info("RIZ:: YarnProjectsQuota entry : " + _yps.getValue().toString());
                        
                        int _totalCharge = 0;
                        for(YarnProjectsQuota _cpq : chargedYarnProjectsQuota){                            
                            if (_cpq.getProjectid().equals(_yps.getValue().getProjectid()))
                            {
                                _totalCharge += _cpq.getCredit();                                
                                
                            }                            
                        }
                        
                        if (_yps.getValue().getCredit() > _totalCharge)
                            deductedYarnProjectsQuota.add(new YarnProjectsQuota(_yps.getValue().getProjectid(),(_yps.getValue().getCredit() - _totalCharge)));
                        else
                            deductedYarnProjectsQuota.add(new YarnProjectsQuota(_yps.getValue().getProjectid(),0));

                        LOG.info("RIZ:: Project : " + _yps.getValue().getProjectid() + " charged for " + _totalCharge + "; ");                        
                    }
                    
                    _pjDA.addAll(deductedYarnProjectsQuota);
                                       
             
                    connector.commit();
                    return null;
                }
              };
            bomb.handle();
            
            
        } catch (StorageInitializtionException ex) {
          LOG.error(ex);
        } catch (StorageException ex) {
          LOG.error(ex);
        }
       
       
    }
    
    
   
}
