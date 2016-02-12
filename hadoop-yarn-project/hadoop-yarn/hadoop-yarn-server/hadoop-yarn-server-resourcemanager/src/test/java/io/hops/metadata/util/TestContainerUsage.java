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

/**
 *
 * @author rizvi
 */
package io.hops.metadata.util;

import io.hops.DalDriver;
import io.hops.StorageConnector;
import io.hops.exception.StorageException;
import io.hops.exception.StorageInitializtionException;
import io.hops.metadata.yarn.TablesDef;
import io.hops.metadata.yarn.dal.ContainerIdToCleanDataAccess;
import io.hops.metadata.yarn.dal.ContainerStatusDataAccess;
import io.hops.metadata.yarn.dal.RMNodeDataAccess;
import io.hops.metadata.yarn.dal.ContainersLogsDataAccess;
import io.hops.metadata.yarn.dal.YarnProjectsDailyCostDataAccess;
import io.hops.metadata.yarn.dal.YarnProjectsQuotaDataAccess;
import io.hops.metadata.yarn.dal.rmstatestore.ApplicationAttemptStateDataAccess;
import io.hops.metadata.yarn.dal.rmstatestore.ApplicationStateDataAccess;
import io.hops.metadata.yarn.dal.util.YARNOperationType;
import io.hops.metadata.yarn.entity.ContainerStatus;
import io.hops.metadata.yarn.entity.RMNode;
import io.hops.metadata.yarn.entity.ContainersLogs;
import io.hops.metadata.yarn.entity.YarnProjectsDailyCost;
import io.hops.metadata.yarn.entity.YarnProjectsDailyId;
import io.hops.metadata.yarn.entity.YarnProjectsQuota;
import io.hops.metadata.yarn.entity.rmstatestore.ApplicationAttemptState;
import io.hops.metadata.yarn.entity.rmstatestore.ApplicationState;
import io.hops.transaction.handler.LightWeightRequestHandler;
import io.hops.transaction.handler.RequestHandler;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.TestResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.codehaus.jackson.map.ext.JodaDeserializers;
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
                    
            YarnAPIStorageFactory.setConfiguration(conf);
            RMStorageFactory.setConfiguration(conf);
            //RMUtilities.InitializeDB(); // It will reset the DB, keep it OFF for some localized test
            
            //this.connector =  RMStorageFactory.getConnector();
            //RequestHandler.setStorageConnector(this.connector);           
            
            /*
            YarnAPIStorageFactory.setConfiguration(conf);
            RMStorageFactory.setConfiguration(conf);
            RMUtilities.InitializeDB();
            */
    }
    
    @Test
    public void TestLocalLogInRM() throws StorageException, IOException{       
        
        try {
            LightWeightRequestHandler bomb;
            bomb = new LightWeightRequestHandler(YARNOperationType.TEST) {
                @Override
                public Object performTask() throws IOException {
                    connector.beginTransaction();
                    connector.writeLock(); 
                    
                    //ContainerIdToCleanClusterJ
                    ContainerIdToCleanDataAccess _csDA = (ContainerIdToCleanDataAccess)RMStorageFactory.getDataAccess(ContainerIdToCleanDataAccess.class);
                    if (_csDA != null){                    
                        List<ContainerId> _containerList =  _csDA.findByRMNode("Andromeda3:51028");
                        for(ContainerId _c :_containerList){
                            LOG.info(_c.toString());

                        }
                    }else
                    {
                        LOG.info("DataAccess failed!");

                    }
                    
                    connector.commit();
                    return null;
                }
            };
            bomb.handle();            
            
        } catch (Exception e) {
            LOG.error(e);
        }
        
        
        
    }
    
    @Test 
    public void TestYarnProjectsDailyCost() throws StorageException, IOException{
                
        try 
        {
            //Date _now = new Date();
            //long _miliSec = _now.getTime(); // Miliseconds since 1970-01-01
            long _miliSec = System.currentTimeMillis();
            //long _miliSec = Calendar.getInstance().getTimeInMillis();
            //final long _day = (((_miliSec / 1000) / 60)/ 60)/ 24;
            final long _day = TimeUnit.DAYS.convert(_miliSec, TimeUnit.MILLISECONDS);
            //long _miliSec = System.nanoTime();  // Not thread safe
            //final long _day = TimeUnit.DAYS.convert(_miliSec, TimeUnit.NANOSECONDS);
                    
            LightWeightRequestHandler bomb;
            bomb = new LightWeightRequestHandler(YARNOperationType.TEST) {
                @Override
                public Object performTask() throws IOException {
                    connector.beginTransaction();
                    connector.writeLock(); 
                    
                    YarnProjectsDailyCostDataAccess _csDA = (YarnProjectsDailyCostDataAccess)RMStorageFactory.getDataAccess(YarnProjectsDailyCostDataAccess.class);
                    if(_csDA !=null ){
                        Map<String, YarnProjectsDailyCost> _pdc =  _csDA.getAll();
                        LOG.info(_pdc.get("TestProject#rizvi#16794").toString());
                        for(  Map.Entry<String , YarnProjectsDailyCost> _p : _pdc.entrySet()){
                            LOG.info("Project daily cost size : " + _p.getValue().toString());
                        }
                    }else
                        LOG.info(">>>> Data Access Problem ... ");
                    
                    
                    List<YarnProjectsDailyCost> hopYarnProjectsDailyCost = new ArrayList<YarnProjectsDailyCost>();
                    hopYarnProjectsDailyCost.add(new YarnProjectsDailyCost("TestProject","rizvi",_day,40));                    
                    _csDA.addAll(hopYarnProjectsDailyCost);
                    //String projectName, String projectUser, long day, int creditsUsed
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
    public void LoadDBforQutaScedulerTest() throws StorageException, IOException{
        LOG.info("--- START: TestContainerUsage ---");
        LOG.info("--- Checking ContainerStatus ---");

        try {   
            
            final List<RMNode> hopRMNode = new ArrayList<RMNode>();
            hopRMNode.add(new RMNode("Andromeda3:51028"));
            /*
            # rmnodeid, hostname, commandport, httpport, nodeaddress, httpaddress, nodeid, healthreport, lasthealthreporttime, currentstate, overcommittimeout, nodemanager_version, uci_id, pendingeventid
            'Andromeda3:51028', 'Andromeda3', '51028', '57120', 'Andromeda3:51028', 'Andromeda3:57120', NULL, '', '1450009406277', 'RUNNING', '-1', '2.4.0', '2', '19'

            */
            
            final List<ContainerStatus> hopContainersStatus = new ArrayList<ContainerStatus>();
            hopContainersStatus.add(new ContainerStatus("container_1450009406746_0001_01_000001", TablesDef.ContainerStatusTableDef.STATE_RUNNING,"",-1000,"Andromeda3:51028",10));
            hopContainersStatus.add(new ContainerStatus("container_1450009406746_0001_02_000001", TablesDef.ContainerStatusTableDef.STATE_RUNNING,"",-1000,"Andromeda3:51028",10));
            hopContainersStatus.add(new ContainerStatus("container_1450009406746_0001_03_000001", TablesDef.ContainerStatusTableDef.STATE_RUNNING,"",-1000,"Andromeda3:51028",10));
            
            //hopContainersStatus.add(new ContainerStatus("container8",TablesDef.ContainerStatusTableDef.STATE_RUNNING,"cont.. from riz", 0, "70", DEFAULT_PENDIND_ID));
            /*          
            # containerid,                            rmnodeid,           state,    diagnostics, exitstatus, pendingeventid
            'container_1450009406746_0001_01_000001', 'Andromeda3:51028', 'RUNNING', '',         '-1000',    '10'
            */
            
            final List<ApplicationState> hopApplicationState = new ArrayList<ApplicationState>();
            hopApplicationState.add(new ApplicationState("application_1450009406746_0001",new byte[0],"Project07__rizvihasan","DistributedShell", "FINISHING"));
            /*
            # applicationid, appstate, appuser, appname, appsmstate
            'application_1450009406746_0001', ?, 'rizvi', 'DistributedShell', 'FINISHING'
            */
            
            final List<ApplicationAttemptState> hopApplicationAttemptState = new ArrayList<ApplicationAttemptState>();
            hopApplicationAttemptState.add(new ApplicationAttemptState("application_1450009406746_0001","appattempt_1450009406746_0001_000001",new byte[0],"Andromeda3/127.0.1.1",-1,null,"http://Andromeda3:44842/proxy/application_1450009406746_0001/A"));
            /*
            # applicationid, applicationattemptid, applicationattemptstate, applicationattempthost, applicationattemptrpcport, applicationattempttokens, applicationattempttrakingurl
            'application_1450009406746_0001', 'appattempt_1450009406746_0001_000001', ?,'Andromeda3/127.0.1.1', '-1', ?, 'http://Andromeda3:44842/proxy/application_1450009406746_0001/A'
            */
            
            final List<ContainersLogs> hopContainersLogs = new ArrayList<ContainersLogs>();
            hopContainersLogs.add(new ContainersLogs("container_1450009406746_0001_01_000001",10,11,ContainerExitStatus.SUCCESS));
            hopContainersLogs.add(new ContainersLogs("container_1450009406746_0001_02_000001",10,11,ContainerExitStatus.ABORTED));
            hopContainersLogs.add(new ContainersLogs("container_1450009406746_0001_03_000001",10,11,ContainerExitStatus.CONTAINER_RUNNING_STATE));
            
            
            final List<YarnProjectsQuota> hopYarnProjectsQuota = new ArrayList<YarnProjectsQuota>();
            hopYarnProjectsQuota.add(new YarnProjectsQuota("Project07",50,0));
            
            
            LightWeightRequestHandler bomb;
            bomb = new LightWeightRequestHandler(YARNOperationType.TEST) {
                @Override
                public Object performTask() throws IOException {
                    connector.beginTransaction();
                    connector.writeLock(); 
                    
                    RMNodeDataAccess _rmDA = (RMNodeDataAccess)RMStorageFactory.getDataAccess(RMNodeDataAccess.class);
                    _rmDA.addAll(hopRMNode);
                    
                    ContainerStatusDataAccess _csDA = (ContainerStatusDataAccess)RMStorageFactory.getDataAccess(ContainerStatusDataAccess.class);
                    _csDA.addAll(hopContainersStatus);
                    
                    ApplicationStateDataAccess<ApplicationState> _appState = (ApplicationStateDataAccess) RMStorageFactory.getDataAccess(ApplicationStateDataAccess.class);
                    _appState.addAll(hopApplicationState);  
                    
                    ApplicationAttemptStateDataAccess<ApplicationAttemptState> _appAttempt = (ApplicationAttemptStateDataAccess) RMStorageFactory.getDataAccess(ApplicationAttemptStateDataAccess.class);
                    _appAttempt.addAll(hopApplicationAttemptState);
                                                          
                    ContainersLogsDataAccess<ContainersLogs> _clDA = (ContainersLogsDataAccess)RMStorageFactory.getDataAccess(ContainersLogsDataAccess.class);
                    _clDA.addAll(hopContainersLogs);
                                        
                    YarnProjectsQuotaDataAccess<YarnProjectsQuota> _pqDA = (YarnProjectsQuotaDataAccess)RMStorageFactory.getDataAccess(YarnProjectsQuotaDataAccess.class);
                    _pqDA.addAll(hopYarnProjectsQuota);
                    
                    
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
    
    
    ApplicationStateDataAccess AppStatDS = null;
    Map<String , ApplicationState>  HopApplicationState = null;
            
    
    @Test
    public void TestContainersLogs() throws StorageException, IOException{
        LOG.info("--- START: TestContainerUsage ---");
        LOG.info("--- Checking ContainersLogs ---");

        try {           
            //Get DataAccess and Map for ** ApplicationState **
            this.AppStatDS = (ApplicationStateDataAccess)RMStorageFactory.getDataAccess(ApplicationStateDataAccess.class);
            this.HopApplicationState = new HashMap<String , ApplicationState>();
                            
            LightWeightRequestHandler bomb = new LightWeightRequestHandler(
              YARNOperationType.TEST) {
                @Override
                public Object performTask() throws IOException {
                    connector.beginTransaction();
                    connector.readLock();
                    
                    
                    //List<ApplicationState> hopApplicationStateList = _appStatDS.getAll();
                    //for ( ApplicationState _as :hopApplicationStateList)
                    //  hopApplicationState.put(_as.getApplicationId(), _as);
                    
                            
                    //Get Data  ** ContainersLogs **
                    ContainersLogsDataAccess _csDA = (ContainersLogsDataAccess)RMStorageFactory.getDataAccess(ContainersLogsDataAccess.class);
                    Map<String , ContainersLogs> hopContainersLogs = _csDA.getAll();
                    
                    //Get Data  ** YarnProjectsQuota **
                    YarnProjectsQuotaDataAccess _pqDA = (YarnProjectsQuotaDataAccess)RMStorageFactory.getDataAccess(YarnProjectsQuotaDataAccess.class);
                    Map<String , YarnProjectsQuota> hopYarnProjectsQuotaList = _pqDA.getAll();
                    //YarnProjectsQuota _pq = (YarnProjectsQuota)hopYarnProjectsQuotaList.get("Project07");
                   
                    //Get Data  ** YarnProjectsDailyCost **
                    YarnProjectsDailyCostDataAccess _pdcDA = (YarnProjectsDailyCostDataAccess)RMStorageFactory.getDataAccess(YarnProjectsDailyCostDataAccess.class);
                    Map<YarnProjectsDailyId , YarnProjectsDailyCost> hopYarnProjectsDailyCostList = _pdcDA.getAll();
                    /*LOG.info("RIZ:: Daily Charged projects... (hopYarnProjectsDailyCostList)");
                    for(Map.Entry<YarnProjectsDailyId, YarnProjectsDailyCost> _cdq : hopYarnProjectsDailyCostList.entrySet()){
                        
                        LOG.info("Key Hashcode: " + _cdq.getKey().hashCode() + 
                                 " Key: " + _cdq.getKey().toString() + 
                                 " Content: " + _cdq.getValue().toString() );
                    }*/
                    
                    
                    
                    long _miliSec = System.currentTimeMillis();            
                    final long _day = TimeUnit.DAYS.convert(_miliSec, TimeUnit.MILLISECONDS);
                    //List<YarnProjectsQuota> chargedYarnProjectsQuota = new ArrayList<YarnProjectsQuota>();
                    Map<String, YarnProjectsQuota> chargedYarnProjectsQuota = new HashMap<String,YarnProjectsQuota>();
                    
                    //List<YarnProjectsDailyCost> chargedYarnProjectsDailyCost = new ArrayList<YarnProjectsDailyCost>();
                    Map<YarnProjectsDailyId, YarnProjectsDailyCost> chargedYarnProjectsDailyCost = new HashMap<YarnProjectsDailyId, YarnProjectsDailyCost>();

                    List<ContainersLogs> toBeRemovedContainersLogs = new ArrayList<ContainersLogs>();
                    List<ContainersLogs> toBeModifiedContainersLogs = new ArrayList<ContainersLogs>();
                    
                    
                    // Calculate the quota 
                    LOG.info("RIZ:: ContainersLogs count : " + hopContainersLogs.size());                    
                    for(  Map.Entry<String , ContainersLogs> _ycl : hopContainersLogs.entrySet()){
                        
                        // Get ApplicationId from ContainerId
                        LOG.info("RIZ:: ContainersLogs entry : " + _ycl.getValue().toString());
                        ContainerId _cId = ConverterUtils.toContainerId(_ycl.getValue().getContainerid());
                        ApplicationId _appId = _cId.getApplicationAttemptId().getApplicationId();                        
                        
                        //Get ProjectId from ApplicationId in ** ApplicationState Table **
                        ApplicationState _appStat = HopApplicationState.get(_appId.toString());
                        if (_appStat == null){
                          _appStat =(ApplicationState)AppStatDS.findByApplicationId(_appId.toString());
                          if (_appStat == null){
                              LOG.error("Application not found: " + _appId.toString());
                          }else{
                              HopApplicationState.put(_appId.toString(),_appStat);                                                        
                          }
                        }
                        //ApplicationState _appStat = hopApplicationState.get(_appId.toString());
                        String _projectid = _appStat.getUser().split("__")[0];
                        String _user = _appStat.getUser().split("__")[1];
                        LOG.info("RIZ:: App : " + _appId.toString() + " User : " + _appStat.getUser());
                                                
                        // Calculate the charge
                        long _charge = _ycl.getValue().getStop() -_ycl.getValue().getStart();
                        
                        // Decide what to do with the charge
                        if (_charge > 0){
                            if (_ycl.getValue().getExitstatus() == ContainerExitStatus.CONTAINER_RUNNING_STATE){
                                //>> Edit log entry + Increase Quota
                                toBeModifiedContainersLogs.add(new ContainersLogs(_ycl.getValue().getContainerid(),
                                                                                          _ycl.getValue().getStop(), 
                                                                                          _ycl.getValue().getStop(),
                                                                                          _ycl.getValue().getExitstatus()));
                                //** YarnProjectsQuota charging**
                                chargeYarnProjectsQuota(chargedYarnProjectsQuota,hopYarnProjectsQuotaList ,_projectid, _charge);
                                
                                //** YarnProjectsDailyCost charging**
                                chargeYarnProjectsDailyCost(chargedYarnProjectsDailyCost,hopYarnProjectsDailyCostList,_projectid, _user, _day, _charge);
                                
                            }
                            else if(_ycl.getValue().getExitstatus() == ContainerExitStatus.ABORTED ||
                                    _ycl.getValue().getExitstatus() == ContainerExitStatus.DISKS_FAILED ||
                                    _ycl.getValue().getExitstatus() == ContainerExitStatus.PREEMPTED){
                                //>> Delete log entry
                                toBeRemovedContainersLogs.add((ContainersLogs)_ycl.getValue());                                
                            }
                            else{
                                //>> Delete log entry + Increase Quota                                   
                                toBeRemovedContainersLogs.add((ContainersLogs)_ycl.getValue());
                                //** YarnProjectsQuota charging**
                                chargeYarnProjectsQuota(chargedYarnProjectsQuota,hopYarnProjectsQuotaList ,_projectid, _charge);
                                
                                //** YarnProjectsDailyCost charging**
                                chargeYarnProjectsDailyCost(chargedYarnProjectsDailyCost,hopYarnProjectsDailyCostList,_projectid, _user, _day, _charge);
                                
                            }
                        }                
                    }
                    
                    // Deleta/Modify the ** ContainersLogs **
                    _csDA.removeAll(toBeRemovedContainersLogs);
                    _csDA.addAll(toBeModifiedContainersLogs);
                    
                    // Show all charged project
                    for(YarnProjectsQuota _cpq : chargedYarnProjectsQuota.values()){                        
                        LOG.info("RIZ:: Charged projects: " + _cpq.toString() + " charge amount:" + _cpq.getTotalUsedQuota());
                    }
                    
                    LOG.info("RIZ:: Daily Charged projects... (chargedYarnProjectsDailyCost)");
                    for(Map.Entry<YarnProjectsDailyId, YarnProjectsDailyCost> _cdq : chargedYarnProjectsDailyCost.entrySet()){
                        
                        LOG.info("Key Hashcode: " + _cdq.getKey().hashCode() + 
                                 " Key: " + _cdq.getKey().toString() + 
                                 " Content: " + _cdq.getValue().toString() );
                    }
                    
                    /*LOG.info("RIZ:: Daily Charged projects... (chargedYarnProjectsDailyCost.values)");
                    for(YarnProjectsDailyCost _cdq : chargedYarnProjectsDailyCost.values()){                        
                        LOG.info(" Content: " + _cdq.toString() );
                    }*/
                    
                    /*LOG.info("RIZ:: Daily Charged projects... (hopYarnProjectsDailyCostList)");
                    for(Map.Entry<YarnProjectsDailyId, YarnProjectsDailyCost> _cdq : hopYarnProjectsDailyCostList.entrySet()){
                        
                        LOG.info("Key Hashcode: " + _cdq.getKey().hashCode() + 
                                 " Key: " + _cdq.getKey().toString() + 
                                 " Content: " + _cdq.getValue().toString() );
                    }*/
                    
                    
                    
                    // RUNNING >> Edit log + Increase Quota
                    //ContainerExitStatus.CONTAINER_RUNNING_STATE (-201)
                    
                    // MACHINE FAULT >> Delete row 
                    //ContainerExitStatus.ABORTED (-100)
                    //ContainerExitStatus.DISKS_FAILED (-101)
                    //ContainerExitStatus.PREEMPTED (-102)      
                    
                    // GRACEFUL SHUTDOWN >> Delete row + Increase Quota
                    //Otherwise -                     
                    
                    _pqDA.addAll(chargedYarnProjectsQuota.values());
                    _pdcDA.addAll(chargedYarnProjectsDailyCost.values());
                                                       
             
                    connector.commit();
                    return null;
                }
                
                private void chargeYarnProjectsQuota(Map<String, YarnProjectsQuota> chargedYarnProjectsQuota,
                                                      Map<String, YarnProjectsQuota> hopYarnProjectsQuotaList,
                                                      String _projectid, long _charge) {
                        
                    YarnProjectsQuota _tempPq = (YarnProjectsQuota)hopYarnProjectsQuotaList.get(_projectid);
                    if(_tempPq != null){
                        YarnProjectsQuota _modifiedPq = new YarnProjectsQuota(_projectid,
                                                                                  _tempPq.getRemainingQuota() - (int)_charge ,
                                                                                  _tempPq.getTotalUsedQuota() + (int)_charge);
                        
                        chargedYarnProjectsQuota.put(_projectid, _modifiedPq);
                        hopYarnProjectsQuotaList.put(_projectid, _modifiedPq);                  
                    }else{
                      LOG.error("Project not found: " + _projectid);
                    }
                      
                }

              

                private void chargeYarnProjectsDailyCost(
                      Map<YarnProjectsDailyId, YarnProjectsDailyCost> chargedYarnProjectsDailyCost,
                      Map<YarnProjectsDailyId, YarnProjectsDailyCost> hopYarnProjectsDailyCostList,
                      String _projectid, String _user, long _day, long _charge) {
                  
                    YarnProjectsDailyId _key = new YarnProjectsDailyId(_projectid, _user, _day);                      
                    YarnProjectsDailyCost _tempPdc =(YarnProjectsDailyCost)hopYarnProjectsDailyCostList.get(_key); // "TestProject#rizvi#16794"
                    if (_tempPdc != null){
                        YarnProjectsDailyCost _incrementedPdc = new  YarnProjectsDailyCost(_projectid, _user, _day, _tempPdc.getCreditsUsed() + (int)_charge);
                        chargedYarnProjectsDailyCost.put(_key,_incrementedPdc);
                        hopYarnProjectsDailyCostList.put(_key,_incrementedPdc);
                    }else{
                        YarnProjectsDailyCost _newPdc = new  YarnProjectsDailyCost(_projectid, _user, _day, (int)_charge);
                        chargedYarnProjectsDailyCost.put(_key, _newPdc);
                        hopYarnProjectsDailyCostList.put(_key, _newPdc);
                    }
                    
                    /*YarnProjectsDailyCost _tempPdc2 =(YarnProjectsDailyCost)chargedYarnProjectsDailyCost.get(_key); // "TestProject#rizvi#16794"
                    if (_tempPdc2 != null){
                        YarnProjectsDailyCost _incrementedPdc = new  YarnProjectsDailyCost(_projectid, _user, _day, _tempPdc2.getCreditsUsed() + (int)_charge);
                        chargedYarnProjectsDailyCost.put(_key,_incrementedPdc);
                    }else{
                        YarnProjectsDailyCost _newPdc = new  YarnProjectsDailyCost(_projectid, _user, _day, (int)_charge);
                        chargedYarnProjectsDailyCost.put(_key, _newPdc);
                    }*/
                }


              };
              bomb.handle();
            
            
        } catch (StorageInitializtionException ex) {
          LOG.error(ex);
        } catch (StorageException ex) {
          LOG.error(ex);
        }
       
       
    }
    
    //@Test
    public void TestProjectSearch() throws StorageException, IOException{
        LOG.info("--- START: TestContainerUsage ---");
        LOG.info("--- Checking ContainersLogs ---");

        try {           
            
            //hopContainersStatus.add(new ContainerStatus("container7",TablesDef.ContainerStatusTableDef.STATE_RUNNING,"cont.. from riz", 0, "70", DEFAULT_PENDIND_ID));
            //hopContainersStatus.add(new ContainerStatus("container8",TablesDef.ContainerStatusTableDef.STATE_RUNNING,"cont.. from riz", 0, "70", DEFAULT_PENDIND_ID));

            
            
            LightWeightRequestHandler bomb = new LightWeightRequestHandler(
              YARNOperationType.TEST) {
                @Override
                public Object performTask() throws IOException {
                    connector.beginTransaction();
                    connector.readLock();
                    
                    //Get Data  ** YarnProjectsQuota **                    
                    YarnProjectsQuotaDataAccess _pqDA = (YarnProjectsQuotaDataAccess)RMStorageFactory.getDataAccess(YarnProjectsQuotaDataAccess.class);
                    
                    Map<String , YarnProjectsQuota> hopYarnProjectsQuota = _pqDA.getAll();
                    YarnProjectsQuota _pq = (YarnProjectsQuota)hopYarnProjectsQuota.get("Project07");
                    LOG.info("Project07 is : " + _pq.toString());
                    
//                    YarnProjectsQuota _pq = (YarnProjectsQuota)_pqDA.findEntry("Project07");
//                    LOG.info("Project07 is : " + _pq.toString());

                    
    
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