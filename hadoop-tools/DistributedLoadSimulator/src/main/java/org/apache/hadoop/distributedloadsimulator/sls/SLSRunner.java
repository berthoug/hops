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
package org.apache.hadoop.distributedloadsimulator.sls;

/**
 *
 * @author sri
 */
import io.hops.metadata.util.RMStorageFactory;
import io.hops.metadata.util.YarnAPIStorageFactory;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import static java.lang.Thread.sleep;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.distributedloadsimulator.sls.appmaster.ApplicationMasterScheduler;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.impl.pb.RpcClientFactoryPBImpl;
import org.apache.hadoop.yarn.server.api.ResourceTracker;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.distributedloadsimulator.sls.utils.SLSUtils;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.map.ObjectMapper;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.distributedloadsimulator.sls.conf.SLSConfiguration;
import org.apache.hadoop.distributedloadsimulator.sls.nodemanager.NMSimulator;
import org.apache.hadoop.distributedloadsimulator.sls.scheduler.ContainerSimulator;
import org.apache.hadoop.distributedloadsimulator.sls.scheduler.ResourceSchedulerWrapper;
import org.apache.hadoop.distributedloadsimulator.sls.scheduler.TaskRunner;
import org.apache.hadoop.yarn.server.resourcemanager.resource.Priority;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.resource.Resources;

public class SLSRunner implements AMNMCommonObject {

    private ResourceManager rm;
    private static final TaskRunner runner = new TaskRunner();
    private final String[] inputTraces;
    private final Configuration conf;
    private final Map<String, Integer> queueAppNumMap;

    // NM simulator
    private static HashMap<NodeId, NMSimulator> nmMap;
    private int nmMemoryMB, nmVCores;
    private final String nodeFile;

    // AM simulator
    private int AM_ID;
    private final Map<String, List<String>> amMap;
    private final Set<String> trackedApps;
    private final Map<String, Class> amClassMap;
    private static int remainingApps = 0;

    // metrics
    private final String metricsOutputDir;
    private final boolean printSimulation;
    private boolean yarnNode = false;
    private boolean firstAMRegistration = false;
    private static boolean distributedmode;
    private final boolean loadsimulatormode;
    private static boolean stopAppSimulation = false;
    private static final boolean calculationDone = false;

    // other simulation information
    private int numNMs, numRacks, numAMs, numTasks;
    private long maxRuntime;
    public final static Map<String, Object> simulateInfoMap
            = new HashMap<String, Object>();

    // logger
    public final static Logger LOG = Logger.getLogger(SLSRunner.class);

    private final String rmAddress;
    private int numberOfRT = 0;
    private final String[] rtAddresses = new String[5]; // this is kind of fixed,
    ResourceTracker[] resourceTrackers = new ResourceTracker[5];

    private int totalJobRunningTimeSec = 0;

    private static float hbResponsePercentage;
    private String rmiAddress = null;

    private static final Map<String, Integer> applicationProcessMap = new HashMap<String, Integer>();

    public SLSRunner(String inputTraces[], String nodeFile,
            String outputDir, Set<String> trackedApps,
            boolean printsimulation, boolean yarnNodeDeployment, boolean distributedMode, boolean loadSimMode, String resourceTrackerAddress, String resourceManagerAddress, String rmiAddress)
            throws IOException, ClassNotFoundException {
        this.rm = null;
        this.yarnNode = yarnNodeDeployment;
        distributedmode = distributedMode;
        this.loadsimulatormode = loadSimMode;
        if (resourceTrackerAddress.split(",").length == 1) { // so we only have one RT
            this.rtAddresses[0] = resourceTrackerAddress;
            this.numberOfRT = 1;
        } else {
            for (int i = 0; i < resourceTrackerAddress.split(",").length; ++i) {
                rtAddresses[i] = resourceTrackerAddress.split(",")[i];
            }
            this.numberOfRT = resourceTrackerAddress.split(",").length;
        }
        this.rmAddress = resourceManagerAddress;
        this.inputTraces = inputTraces.clone();
        this.nodeFile = nodeFile;
        this.trackedApps = trackedApps;
        this.printSimulation = printsimulation;
        metricsOutputDir = outputDir;
        this.rmiAddress = rmiAddress;

        nmMap = new HashMap<NodeId, NMSimulator>();
        queueAppNumMap = new HashMap<String, Integer>();
        amMap = new HashMap<String, List<String>>();
        amClassMap = new HashMap<String, Class>();

        // runner configuration
        conf = new Configuration(false);
        conf.addResource("sls-runner.xml");
        // runner
        int poolSize = conf.getInt(SLSConfiguration.RUNNER_POOL_SIZE,
                SLSConfiguration.RUNNER_POOL_SIZE_DEFAULT);
        SLSRunner.runner.setQueueSize(poolSize);
        // <AMType, Class> map
        for (Map.Entry e : conf) {
            String key = e.getKey().toString();
            if (key.startsWith(SLSConfiguration.AM_TYPE)) {
                String amType = key.substring(SLSConfiguration.AM_TYPE.length());
                amClassMap.put(amType, Class.forName(conf.get(key)));
            }
        }
    }

    public static void addProcessId(String appId, int processId) {
        LOG.info(MessageFormat.format("Adding the process id in to map {0}  - {1}", appId, processId));
        applicationProcessMap.put(appId, processId);
    }

    private String getRMIAddress() {
        return this.rmiAddress;
    }

    public void startHbMonitorThread() {
        Thread hbExperimentalMonitoring = new Thread() {
            @Override
            public void run() {
                while (true) {
                    try {
                        sleep(5000);
                    } catch (InterruptedException ex) {
                        java.util.logging.Logger.getLogger(SLSRunner.class.getName()).log(Level.SEVERE, null, ex);
                    }
                    int totalHb = 0;
                    int trueTotalHb = 0;
                    for (NMSimulator nm : nmMap.values()) {
                        totalHb += nm.getTotalHeartBeat();
                        trueTotalHb += nm.getTotalTrueHeartBeat();
                    }
                    if (totalHb != 0) {
                        float hbExperimentailResponsePercentage = (trueTotalHb * 100) / totalHb;
                        LOG.info("Experimental hb response : " + hbExperimentailResponsePercentage + " " + nmMap.size() + " " + numAMs + " " + totalHb + " " + trueTotalHb + " " + totalJobRunningTimeSec);
                    }
                }
            }
        };
        hbExperimentalMonitoring.start();
    }

    public void start() throws Exception {
        if (loadsimulatormode) {
            // here we only need to start the load and send rt and scheduler
            startNM();
            // start application masters
            if (!stopAppSimulation) {
                LOG.info("Starting the applicatoin simulator from ApplicationMaster traces");
                startAMFromSLSTraces();
            }
            numAMs = amMap.size();
            remainingApps = numAMs;
            // this method will be used for only experimental purpose. Every 5 sec , it will print the hb handled percentage
            //just to get some idea about the experiment.
            startHbMonitorThread();
        } else if (distributedmode) {
            // before start the rm , let rm to read and get to know about number of applications
            startAMFromSLSTraces();
            startRM();
            ((ResourceSchedulerWrapper) rm.getResourceScheduler())
                    .setQueueSet(this.queueAppNumMap.keySet());
            ((ResourceSchedulerWrapper) rm.getResourceScheduler())
                    .setTrackedAppSet(this.trackedApps);
        }
        printSimulationInfo();
        runner.start();
    }

    private void startRM() throws IOException, ClassNotFoundException {
        Configuration rmConf = new YarnConfiguration();

        rmConf.setBoolean(YarnConfiguration.RM_HA_ENABLED, true);
        rmConf.setBoolean(YarnConfiguration.HOPS_DISTRIBUTED_RT_ENABLED, true);
        rmConf.setBoolean(YarnConfiguration.HOPS_NDB_EVENT_STREAMING_ENABLED, true);
        rmConf.setBoolean(YarnConfiguration.HOPS_NDB_RT_EVENT_STREAMING_ENABLED, true);
        rmConf.setBoolean(YarnConfiguration.AUTO_FAILOVER_ENABLED, true);
        LOG.info("HOP :: Load simulator is starting resource manager in distributed mode ######################### ");

        YarnAPIStorageFactory.setConfiguration(rmConf);
        RMStorageFactory.setConfiguration(rmConf);

        String schedulerClass = rmConf.get(YarnConfiguration.RM_SCHEDULER);
        rmConf.set(SLSConfiguration.RM_SCHEDULER, schedulerClass);
        rmConf.set(YarnConfiguration.RM_SCHEDULER,
                ResourceSchedulerWrapper.class.getName());
        rmConf.set(SLSConfiguration.METRICS_OUTPUT_DIR, metricsOutputDir);
        rm = new ResourceManager();
        rm.init(rmConf);
        rm.start();
    }

    private void startNM() throws YarnException, IOException {
        // nm configuration
        // 38GB
        nmMemoryMB = conf.getInt(SLSConfiguration.NM_MEMORY_MB,
                SLSConfiguration.NM_MEMORY_MB_DEFAULT);
        nmVCores = conf.getInt(SLSConfiguration.NM_VCORES,
                SLSConfiguration.NM_VCORES_DEFAULT);
        int heartbeatInterval = conf.getInt(
                SLSConfiguration.NM_HEARTBEAT_INTERVAL_MS,
                SLSConfiguration.NM_HEARTBEAT_INTERVAL_MS_DEFAULT);
        // nm information (fetch from topology file, or from sls/rumen json file)
        Set<String> nodeSet = new HashSet<String>();
        if (nodeFile.isEmpty()) {
            for (String inputTrace : inputTraces) {
                nodeSet.addAll(SLSUtils.parseNodesFromSLSTrace(inputTrace));
            }

        } else {
            nodeSet.addAll(SLSUtils.parseNodesFromNodeFile(nodeFile));
        }

        Configuration rtConf = new YarnConfiguration();
        int rtPort = rtConf.getPort(YarnConfiguration.RM_RESOURCE_TRACKER_PORT, YarnConfiguration.DEFAULT_RM_RESOURCE_TRACKER_PORT);

        for (int i = 0; i < numberOfRT; ++i) {
            rtConf.setStrings(YarnConfiguration.RM_RESOURCE_TRACKER_ADDRESS, rtAddresses[i]);
            resourceTrackers[i] = (ResourceTracker) RpcClientFactoryPBImpl.get().
                    getClient(ResourceTracker.class, 1, rtConf.getSocketAddr(
                                    YarnConfiguration.RM_RESOURCE_TRACKER_ADDRESS,
                                    YarnConfiguration.DEFAULT_RM_RESOURCE_TRACKER_ADDRESS,
                                    rtPort), rtConf);
        }
        // create NM simulators
        int counter = 0;
        Random random = new Random();
        Set<String> rackSet = new HashSet<String>();
        int totalNodes = nodeSet.size();
        int threshHoldLimit = totalNodes / numberOfRT;
        int rtOffSet = 0;
        int i=0;
        for (String hostName : nodeSet) {
            ++counter;
            // we randomize the heartbeat start time from zero to 1 interval
          i++;
          LOG.info("Init nm: " + hostName + " (" + i + ")");
            NMSimulator nm = new NMSimulator();
            if (counter <= threshHoldLimit) {
                nm.init(hostName, nmMemoryMB, nmVCores,
                        random.nextInt(heartbeatInterval), heartbeatInterval, rm, resourceTrackers[rtOffSet]);

            }
            if (counter == threshHoldLimit) {
                threshHoldLimit *= 2;
                ++rtOffSet;
            }
            nmMap.put(nm.getNode().getNodeID(), nm);
            runner.schedule(nm);
            rackSet.add(nm.getNode().getRackName());

        }
        numRacks = rackSet.size();
        numNMs = nmMap.size();
    }

    /**
     * parse workload information from sls trace files
     */
    @SuppressWarnings("unchecked")
    private void startAMFromSLSTraces() throws IOException, Exception {
        // parse from sls traces
        int heartbeatInterval = conf.getInt(
                SLSConfiguration.AM_HEARTBEAT_INTERVAL_MS,
                SLSConfiguration.AM_HEARTBEAT_INTERVAL_MS_DEFAULT);
        JsonFactory jsonF = new JsonFactory();
        ObjectMapper mapper = new ObjectMapper();
        for (String inputTrace : inputTraces) {
            Reader input = new FileReader(inputTrace);
            try {
                Iterator<Map> i = mapper.readValues(jsonF.createJsonParser(input),
                        Map.class);
                while (i.hasNext()) {
                    Map jsonJob = i.next();

                    long jobStartTime = Long.parseLong(
                            jsonJob.get("job.start.ms").toString());
                    long jobFinishTime = Long.parseLong(
                            jsonJob.get("job.end.ms").toString());

                    String queue = jsonJob.get("job.queue.name").toString();

                    String oldAppId = jsonJob.get("job.id").toString();
                    totalJobRunningTimeSec = (int) jobFinishTime / 1000;// every time we update the time, so final time is total time
                    int queueSize = queueAppNumMap.containsKey(queue)
                            ? queueAppNumMap.get(queue) : 0;
                    queueSize++;
                    queueAppNumMap.put(queue, queueSize);
                    // tasks
                    List tasks = (List) jsonJob.get("job.tasks");
                    if (tasks == null || tasks.isEmpty()) {
                        continue;
                    }
                    List<ContainerSimulator> containerList
                            = new ArrayList<ContainerSimulator>();
                    // create a new AM
                    // appMastersList.add(new AppMasterParameter(queue, inputTrace, AM_ID++, rmAddress, rmiAddress));
                    // if it is yarn node, don't execute applications
                    if (!yarnNode) {
                        LOG.info("Application simulation is starting now .");
                        ApplicationMasterScheduler appMasterExecutors = new ApplicationMasterScheduler(queue, inputTrace, AM_ID++, rmAddress, rmiAddress);
                        appMasterExecutors.init(jobStartTime, jobFinishTime, heartbeatInterval);
                        runner.schedule(appMasterExecutors);
                    }
                    String amType = jsonJob.get("am.type").toString();
                    maxRuntime = Math.max(maxRuntime, jobFinishTime);
                    numTasks += tasks.size();
                    List<String> amList = new ArrayList<String>();
                    amList.add(queue);
                    amList.add(amType);
                    amList.add(Long.toString(jobFinishTime - jobStartTime));
                    amList.add(Integer.toString(containerList.size()));
                    amMap.put(oldAppId, amList);
                }
            } finally {
                input.close();
            }
        }
        numAMs = amMap.size();
        remainingApps = numAMs;
    }

    private void printSimulationInfo() {
        if (printSimulation) {
            // node
            LOG.info("------------------------------------");
            LOG.info(MessageFormat.format("# nodes = {0}, # racks = {1}, capacity "
                    + "of each node {2} MB memory and {3} vcores.",
                    numNMs, numRacks, nmMemoryMB, nmVCores));
            LOG.info("------------------------------------");
            // job
            LOG.info(MessageFormat.format("# applications = {0}, # total "
                    + "tasks = {1}, average # tasks per application = {2}",
                    numAMs, numTasks, (int) (Math.ceil((numTasks + 0.0) / numAMs))));
            LOG.info("JobId\tQueue\tAMType\tDuration\t#Tasks");
            for (Map.Entry<String, List<String>> entry : amMap.entrySet()) {
                List<String> amDetail = entry.getValue();
                LOG.info(entry.getKey() + "\t" + amDetail.get(0) + "\t" + amDetail.get(1)
                        + "\t" + amDetail.get(2) + "\t" + amDetail.get(3));
            }
            LOG.info("------------------------------------");
            // queue
            LOG.info(MessageFormat.format("number of queues = {0}  average "
                    + "number of apps = {1}", queueAppNumMap.size(),
                    (int) (Math.ceil((numAMs + 0.0) / queueAppNumMap.size()))));
            LOG.info("------------------------------------");
            // runtime
            LOG.info(MessageFormat.format("estimated simulation time is {0}"
                    + " seconds", (long) (Math.ceil(maxRuntime / 1000.0))));
            LOG.info("------------------------------------");
        }
        // package these information in the simulateInfoMap used by other places
        simulateInfoMap.put("Number of racks", numRacks);
        simulateInfoMap.put("Number of nodes", numNMs);
        simulateInfoMap.put("Node memory (MB)", nmMemoryMB);
        simulateInfoMap.put("Node VCores", nmVCores);
        simulateInfoMap.put("Number of applications", numAMs);
        simulateInfoMap.put("Number of tasks", numTasks);
        simulateInfoMap.put("Average tasks per applicaion",
                (int) (Math.ceil((numTasks + 0.0) / numAMs)));
        simulateInfoMap.put("Number of queues", queueAppNumMap.size());
        simulateInfoMap.put("Average applications per queue",
                (int) (Math.ceil((numAMs + 0.0) / queueAppNumMap.size())));
        simulateInfoMap.put("Estimated simulate time (s)",
                (long) (Math.ceil(maxRuntime / 1000.0)));
    }

    public HashMap<NodeId, NMSimulator> getNmMap() {
        return nmMap;
    }

    public static TaskRunner getRunner() {
        return runner;
    }

    public static void decreaseRemainingApps() {
        remainingApps--;
        LOG.info("SLS decrease finished application - application count : " + remainingApps);
        if (remainingApps == 0) {
            LOG.info("<SLSisShuttingDown>");
            // if distributed mode enabled , then no point of calculating from rm
            if (!distributedmode) {
                while (!calculationDone) {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException ex) {
                        java.util.logging.Logger.getLogger(SLSRunner.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
            }
            System.exit(0);
        }
    }

    public static void main(String args[]) throws Exception {
        Options options = new Options();
        options.addOption("inputsls", true, "input sls files");
        options.addOption("nodes", true, "input topology");
        options.addOption("output", true, "output directory");
        options.addOption("trackjobs", true,
                "jobs to be tracked during simulating");
        options.addOption("printsimulation", false,
                "print out simulation information");
        options.addOption("yarnnode", false, "taking boolean to enable rt mode");
        options.addOption("distributedmode", false, "taking boolean to enable scheduler mode");
        options.addOption("loadsimulatormode", false, "taking boolean to enable load simulator mode");
        options.addOption("rtaddress", true, "Resourcetracker address");
        options.addOption("rmaddress", true, "Resourcemanager  address for appmaster");
        options.addOption("parallelsimulator", false, "this is a boolean value to check whether to enable parallel simulator or not");
        options.addOption("rmiaddress", true, "Run a simulator on distributed mode, so we need rmi address");
        options.addOption("stopappsimulation", false, "we can stop the application simulation");

        CommandLineParser parser = new GnuParser();
        CommandLine cmd = parser.parse(options, args);

        String inputSLS = cmd.getOptionValue("inputsls");
        String output = cmd.getOptionValue("output");
        String rtAddress = cmd.getOptionValue("rtaddress"); // we are expecting the multiple rt, so input should be comma seperated
        String rmAddress = cmd.getOptionValue("rmaddress");
        String rmiAddress = "127.0.0.1";

        if ((inputSLS == null) || output == null) {
            System.err.println();
            System.err.println("ERROR: Missing input or output file");
            System.err.println();
            System.err.println("Options: -inputsls FILE,FILE... "
                    + "-output FILE [-nodes FILE] [-trackjobs JobId,JobId...] "
                    + "[-printsimulation]" + "[-distributedrt]");
            System.err.println();
            System.exit(1);
        }

        File outputFile = new File(output);
        if (!outputFile.exists()
                && !outputFile.mkdirs()) {
            System.err.println("ERROR: Cannot create output directory "
                    + outputFile.getAbsolutePath());
            System.exit(1);
        }

        Set<String> trackedJobSet = new HashSet<String>();
        if (cmd.hasOption("trackjobs")) {
            String trackjobs = cmd.getOptionValue("trackjobs");
            String jobIds[] = trackjobs.split(",");
            trackedJobSet.addAll(Arrays.asList(jobIds));
        }

        String nodeFile = cmd.hasOption("nodes") ? cmd.getOptionValue("nodes") : "";

        String inputFiles[] = inputSLS.split(",");
        if (cmd.hasOption("stopappsimulation")) {
            stopAppSimulation = true;
            LOG.warn("Application simulation is disabled!!!!!!");
        }
        if (cmd.hasOption("parallelsimulator")) {
            //  then we need rmi address
            rmiAddress = cmd.getOptionValue("rmiaddress"); // currently we support only two simulator in parallel
        }
        SLSRunner sls = new SLSRunner(inputFiles, nodeFile, output,
                trackedJobSet, cmd.hasOption("printsimulation"), cmd.hasOption("yarnnode"), cmd.hasOption("distributedmode"), cmd.hasOption("loadsimulatormode"), rtAddress, rmAddress, rmiAddress
        );
        if (!cmd.hasOption("distributedmode")) {
            try {
                AMNMCommonObject stub = (AMNMCommonObject) UnicastRemoteObject.exportObject(sls, 0);
                // Bind the remote object's stub in the registry
                Registry registry = LocateRegistry.getRegistry();
                registry.bind("AMNMCommonObject", stub);
                LOG.info("HOP ::  SLS RMI Server ready on default RMI port ");
                sls.start();
            } catch (Exception e) {
                System.err.println("Server exception: " + e.toString());
                e.printStackTrace();
            }
        } else {
            sls.start();
        }
    }

    @Override
    public boolean isNodeExist(String nodeId) throws RemoteException {
        NodeId nId = ConverterUtils.toNodeId(nodeId);
        if (nmMap.containsKey(nId)) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void addNewContainer(String containerId, String nodeId, String httpAddress,
            int memory, int vcores, int priority, long lifeTimeMS) throws RemoteException {
        Container container
                = BuilderUtils.newContainer(ConverterUtils.toContainerId(containerId),
                        ConverterUtils.toNodeId(nodeId), httpAddress,
                        Resources.createResource(memory, vcores),
                        Priority.create(priority), null);

        // this we can move to thread queue to increase the performance, so we don't need to wait
        nmMap.get(container.getNodeId())
                .addNewContainer(container, lifeTimeMS);
    }

    @Override
    public void cleanupContainer(String containerId, String nodeId) throws RemoteException {
        nmMap.get(ConverterUtils.toNodeId(nodeId))
                .cleanupContainer(ConverterUtils.toContainerId(containerId));
    }

    @Override
    public int finishedApplicationsCount() {
        return remainingApps;
    }

    @Override
    public void registerApplicationTimeStamp() {
        if (!firstAMRegistration) {
            LOG.info("Application_initial_registeration_time : " + System.currentTimeMillis());
            firstAMRegistration = true;
        }
    }

    @Override
    public void decreseApplicationCount(String applicationId) {

        if (!yarnNode) {
            remainingApps--;
            LOG.info("SLS decrease finished application - application count : " + remainingApps);

            int processId = applicationProcessMap.get(applicationId);
            String killProcess = "kill -9 " + processId;
            LOG.info(MessageFormat.format("Application {0} is killing now , kill process string : {1} ", applicationId, killProcess));
            try {
                Process amProc = Runtime.getRuntime().exec(killProcess);
            } catch (IOException ex) {
                java.util.logging.Logger.getLogger(SLSRunner.class.getName()).log(Level.SEVERE, null, ex);
            }

            if (remainingApps == 0) {
                LOG.info("Distributed_Simulator_shutting_down_time : " + System.currentTimeMillis());
                //now check whether other simulator is finished 
                Registry secondryRegistry = null;
                try {
                    secondryRegistry = LocateRegistry.getRegistry(getRMIAddress());
                } catch (RemoteException ex) {
                    java.util.logging.Logger.getLogger(SLSRunner.class.getName()).log(Level.SEVERE, null, ex);
                }

                try {
                    AMNMCommonObject secondryRemoteConnection = (AMNMCommonObject) secondryRegistry.lookup("AMNMCommonObject");
                    while (secondryRemoteConnection.finishedApplicationsCount() != 0) {
                        Thread.sleep(1000);
                    }
                } catch (RemoteException ex) {
                    java.util.logging.Logger.getLogger(SLSRunner.class.getName()).log(Level.SEVERE, null, ex);
                } catch (NotBoundException ex) {
                    java.util.logging.Logger.getLogger(SLSRunner.class.getName()).log(Level.SEVERE, null, ex);
                } catch (InterruptedException ex) {
                    java.util.logging.Logger.getLogger(SLSRunner.class.getName()).log(Level.SEVERE, null, ex);
                }

                try {
                    // just sleep here, without this sleep , it is sometime possible, other side requestion rmi
                    //function which this exit
                    Thread.sleep(3000);
                } catch (InterruptedException ex) {
                    java.util.logging.Logger.getLogger(SLSRunner.class.getName()).log(Level.SEVERE, null, ex);
                }
                int totalHb = 0;
                int trueTotalHb = 0;
                for (NMSimulator nm : nmMap.values()) {
                    totalHb += nm.getTotalHeartBeat();
                    trueTotalHb += nm.getTotalTrueHeartBeat();
                }
                hbResponsePercentage = (trueTotalHb * 100) / totalHb;
                LOG.info("================== Result format:hpresponsepercentage,nmsize,amsize,totalhb,truetotalhb,totaljobrunningtieminsec ==================");
                LOG.info("Simulation: " + hbResponsePercentage + " " + nmMap.size() + " " + numAMs + " " + totalHb + " " + trueTotalHb + " " + totalJobRunningTimeSec);
                System.exit(0);
            }
        }
    }
}
