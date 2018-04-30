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
package org.apache.hadoop.hdfs.server.datanode;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.StorageType;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi.ScanInfo;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.Time;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.*;

/**
 * Periodically scans the data directories for block and block metadata files.
 * Reconciles the differences with block information maintained in the dataset.
 */
@InterfaceAudience.Private
public class DirectoryScanner implements Runnable {
  private static final Log LOG = LogFactory.getLog(DirectoryScanner.class);

  private final FsDatasetSpi<?> dataset;
  private final ExecutorService reportCompileThreadPool;
  private final ScheduledExecutorService masterThread;
  private final long scanPeriodMsecs;
  private volatile boolean shouldRun = false;
  private boolean retainDiffs = false;

  ScanInfoPerBlockPool diffs = new ScanInfoPerBlockPool();
  Map<String, Stats> stats = new HashMap<>();
  
  /**
   * Allow retaining diffs for unit test and analysis
   *
   * @param b
   *     - defaults to false (off)
   */
  void setRetainDiffs(boolean b) {
    retainDiffs = b;
  }

  /**
   * Stats tracked for reporting and testing, per blockpool
   */
  static class Stats {
    String bpid;
    long totalBlocks = 0;
    long missingMetaFile = 0;
    long missingBlockFile = 0;
    long missingMemoryBlocks = 0;
    long mismatchBlocks = 0;

    public Stats(String bpid) {
      this.bpid = bpid;
    }
    
    @Override
    public String toString() {
      return "BlockPool " + bpid + " Total blocks: " + totalBlocks +
          ", missing metadata files:" + missingMetaFile +
          ", missing block files:" + missingBlockFile +
          ", missing blocks in memory:" + missingMemoryBlocks +
          ", mismatched blocks:" + mismatchBlocks;
    }
  }

  static class ScanInfoPerBlockPool
      extends HashMap<String, LinkedList<ScanInfo>> {
    
    private static final long serialVersionUID = 1L;

    ScanInfoPerBlockPool() {
      super();
    }

    ScanInfoPerBlockPool(int sz) {
      super(sz);
    }
    
    /**
     * Merges "that" ScanInfoPerBlockPool into this one
     *
     * @param that
     */
    public void addAll(ScanInfoPerBlockPool that) {
      if (that == null) {
        return;
      }
      
      for (Entry<String, LinkedList<ScanInfo>> entry : that.entrySet()) {
        String bpid = entry.getKey();
        LinkedList<ScanInfo> list = entry.getValue();
        
        if (this.containsKey(bpid)) {
          //merge that per-bpid linked list with this one
          this.get(bpid).addAll(list);
        } else {
          //add that new bpid and its linked list to this
          this.put(bpid, list);
        }
      }
    }
    
    /**
     * Convert all the LinkedList values in this ScanInfoPerBlockPool map
     * into sorted arrays, and return a new map of these arrays per blockpool
     *
     * @return a map of ScanInfo arrays per blockpool
     */
    public Map<String, ScanInfo[]> toSortedArrays() {
      Map<String, ScanInfo[]> result =
          new HashMap<>(this.size());
      
      for (Entry<String, LinkedList<ScanInfo>> entry : this.entrySet()) {
        String bpid = entry.getKey();
        LinkedList<ScanInfo> list = entry.getValue();
        
        // convert list to array
        ScanInfo[] record = list.toArray(new ScanInfo[list.size()]);
        // Sort array based on blockId
        Arrays.sort(record);
        result.put(bpid, record);
      }
      return result;
    }
  }


  public DirectoryScanner(FsDatasetSpi<?> dataset, Configuration conf) {
    this.dataset = dataset;
    int interval =
        conf.getInt(DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_INTERVAL_KEY,
            DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_INTERVAL_DEFAULT);
    scanPeriodMsecs = interval * 1000L; //msec
    int threads =
        conf.getInt(DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_THREADS_KEY,
            DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_THREADS_DEFAULT);

    reportCompileThreadPool =
        Executors.newFixedThreadPool(threads, new Daemon.DaemonFactory());
    masterThread =
        new ScheduledThreadPoolExecutor(1, new Daemon.DaemonFactory());
  }

  void start() {
    shouldRun = true;
    long offset = DFSUtil.getRandom().nextInt((int) (scanPeriodMsecs / 1000L)) *
        1000L; //msec
    long firstScanTime = Time.now() + offset;
    LOG.info("Periodic Directory Tree Verification scan starting at " +
        firstScanTime + " with interval " + scanPeriodMsecs);
    masterThread.scheduleAtFixedRate(this, offset, scanPeriodMsecs,
        TimeUnit.MILLISECONDS);
  }
  
  // for unit test
  boolean getRunStatus() {
    return shouldRun;
  }

  private void clear() {
    diffs.clear();
    stats.clear();
  }

  /**
   * Main program loop for DirectoryScanner
   * Runs "reconcile()" periodically under the masterThread.
   */
  @Override
  public void run() {
    try {
      if (!shouldRun) {
        //shutdown has been activated
        LOG.warn(
            "this cycle terminating immediately because 'shouldRun' has been deactivated");
        return;
      }

      //We're are okay to run - do it
      reconcile();
      
    } catch (Exception e) {
      //Log and continue - allows Executor to run again next cycle
      LOG.error(
          "Exception during DirectoryScanner execution - will continue next cycle",
          e);
    } catch (Error er) {
      //Non-recoverable error - re-throw after logging the problem
      LOG.error(
          "System Error during DirectoryScanner execution - permanently terminating periodic scanner",
          er);
      throw er;
    }
  }

  void shutdown() {
    if (!shouldRun) {
      LOG.warn(
          "DirectoryScanner: shutdown has been called, but periodic scanner not started");
    } else {
      LOG.warn("DirectoryScanner: shutdown has been called"); // TODO: GABRIE: - sould be calledd?
    }
    shouldRun = false;
    if (masterThread != null) {
      masterThread.shutdown();
    }
    if (reportCompileThreadPool != null) {
      reportCompileThreadPool.shutdown();
    }
    if (masterThread != null) {
      try {
        masterThread.awaitTermination(1, TimeUnit.MINUTES);
      } catch (InterruptedException e) {
        LOG.error(
            "interrupted while waiting for masterThread to " + "terminate", e);
      }
    }
    if (reportCompileThreadPool != null) {
      try {
        reportCompileThreadPool.awaitTermination(1, TimeUnit.MINUTES);
      } catch (InterruptedException e) {
        LOG.error("interrupted while waiting for reportCompileThreadPool to " +
            "terminate", e);
      }
    }
    if (!retainDiffs) {
      clear();
    }
  }

  /**
   * Reconcile differences between disk and in-memory blocks
   */
  void reconcile() {
    scan();
    for (Entry<String, LinkedList<ScanInfo>> entry : diffs.entrySet()) {
      String bpid = entry.getKey();
      LinkedList<ScanInfo> diff = entry.getValue();
      
      for (ScanInfo info : diff) {
        try {
          dataset.checkAndUpdate(bpid, info);
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
    if (!retainDiffs) {
      clear();
    }
  }

  /**
   * Scan for the differences between disk and in-memory blocks
   * Scan only the "finalized blocks" lists of both disk and memory.
   */
  void scan() {
    clear();
    Map<String, ScanInfo[]> diskReport = getDiskReport();

    // Hold FSDataset lock to prevent further changes to the block map
    synchronized (dataset) {
      for (Entry<String, ScanInfo[]> entry : diskReport.entrySet()) {
        String bpid = entry.getKey();
        ScanInfo[] blockpoolReport = entry.getValue();
        
        Stats statsRecord = new Stats(bpid);
        stats.put(bpid, statsRecord);
        LinkedList<ScanInfo> diffRecord = new LinkedList<>();
        diffs.put(bpid, diffRecord);
        
        statsRecord.totalBlocks = blockpoolReport.length;
        List<ReplicaInfo> bl = dataset.getFinalizedBlocks(bpid);
        FinalizedReplica[] memReport = bl.toArray(new FinalizedReplica[bl.size()]);
        Arrays.sort(memReport); // Sort based on blockId

        int d = 0; // index for blockpoolReport
        int m = 0; // index for memReprot
        while (m < memReport.length && d < blockpoolReport.length) {
          Block memBlock = memReport[Math.min(m, memReport.length - 1)];
          ScanInfo info =
              blockpoolReport[Math.min(d, blockpoolReport.length - 1)];
          if (info.getBlockId() < memBlock.getBlockId()) {
            // Block is missing in memory
            statsRecord.missingMemoryBlocks++;
            addDifference(diffRecord, statsRecord, info);
            d++;
            continue;
          }
          if (info.getBlockId() > memBlock.getBlockId()) {
            // Block is missing on the disk
            addDifference(diffRecord, statsRecord, memBlock.getBlockId(),
                info.getVolume());
            m++;
            continue;
          }
          // Block file and/or metadata file exists on the disk
          // Block exists in memory
          if (info.getVolume().getStorageType() != StorageType.PROVIDED &&
              info.getBlockFile() == null) {
            // Block metadata file exits and block file is missing
            addDifference(diffRecord, statsRecord, info);
          } else if (info.getGenStamp() != memBlock.getGenerationStamp() ||
              info.getBlockFile().length() != memBlock.getNumBytes()) {
            // Block metadata file is missing or has wrong generation stamp,
            // or block file length is different than expected
            statsRecord.mismatchBlocks++;
            addDifference(diffRecord, statsRecord, info);
          }
          d++;
          m++;
        }
        while (m < memReport.length) {
          FinalizedReplica current = memReport[m++];
          addDifference(diffRecord, statsRecord,
              current.getBlockId(), current.getVolume());
        }
        while (d < blockpoolReport.length) {
          statsRecord.missingMemoryBlocks++;
          addDifference(diffRecord, statsRecord, blockpoolReport[d++]);
        }
        LOG.info(statsRecord.toString());
      } //end for
    } //end synchronizedz
  }

  /**
   * Block is found on the disk. In-memory block is missing or does not match
   * the block on the disk
   */
  private void addDifference(LinkedList<ScanInfo> diffRecord, Stats statsRecord,
      ScanInfo info) {
    statsRecord.missingMetaFile += info.getMetaFile() == null ? 1 : 0;
    statsRecord.missingBlockFile += info.getBlockFile() == null ? 1 : 0;
    diffRecord.add(info);
  }

  /**
   * Block is not found on the disk
   */
  private void addDifference(LinkedList<ScanInfo> diffRecord, Stats statsRecord,
      long blockId, FsVolumeSpi vol) {
    statsRecord.missingBlockFile++;
    statsRecord.missingMetaFile++;
    diffRecord.add(new ScanInfo(blockId, null, null, vol));
  }

  /**
   * Is the given volume still valid in the dataset?
   */
  private static boolean isValid(final FsDatasetSpi<?> dataset,
      final FsVolumeSpi volume) {
      for (FsVolumeSpi vol : dataset.getVolumes()) {
      if (vol.getStorageType() == StorageType.PROVIDED) {
        // Disable scanning PROVIDED volumes to keep overhead low
        return false;
      }
      if (vol == volume) {
        return true;
      }
    }
    return false;
  }

  /**
   * Get lists of blocks on the disk sorted by blockId, per blockpool
   */
  public Map<String, ScanInfo[]> getDiskReport() {
    // First get list of data directories
    final List<? extends FsVolumeSpi> volumes = dataset.getVolumes();

    // Use an array since the threads may return out of order and
    // compilersInProgress#keySet may return out of order as well.
    ScanInfoPerBlockPool[] dirReports =
            new ScanInfoPerBlockPool[volumes.size()];

    Map<Integer, Future<ScanInfoPerBlockPool>> compilersInProgress =
            new HashMap<>();

    for (int i = 0; i < volumes.size(); i++) {
      if (volumes.get(i).getStorageType() == StorageType.PROVIDED) {
        // Disable scanning PROVIDED volumes to keep overhead low
        continue;
      }
      // if (isValid(dataset, volumes.get(i))) {
      ReportCompiler reportCompiler = new ReportCompiler(volumes.get(i));
      Future<ScanInfoPerBlockPool> result =
              reportCompileThreadPool.submit(reportCompiler);
      compilersInProgress.put(i, result);
      //  }
    }

    for (Entry<Integer, Future<ScanInfoPerBlockPool>> report :
            compilersInProgress.entrySet()) {
      Integer index = report.getKey();
      try {
        dirReports[index] = report.getValue().get();
        // If our compiler threads were interrupted, give up on this run
        if (dirReports[index] == null) {
          dirReports = null;
          break;
        }
      } catch (Exception ex) {
        LOG.error("Error compiling report", ex);
        // Propagate ex to DataBlockScanner to deal with
        throw new RuntimeException(ex);
      }
    }
  // Compile consolidated report for all the volumes
  ScanInfoPerBlockPool list = new ScanInfoPerBlockPool();
  for (int i = 0; i < volumes.size(); i++) {
    if (dirReports[i] != null) {
      // volume is still valid
      list.addAll(dirReports[i]);
    }
  }
    return list.toSortedArrays();
  }

  public static class ReportCompiler
      implements Callable<ScanInfoPerBlockPool> {
    private FsVolumeSpi volume;

    public ReportCompiler(FsVolumeSpi volume) {
      this.volume = volume;
    }

    @Override
    public ScanInfoPerBlockPool call() throws Exception {
      String[] bpList = volume.getBlockPoolList();
      ScanInfoPerBlockPool result = new ScanInfoPerBlockPool(bpList.length);
      for (String bpid : bpList) {
        LinkedList<ScanInfo> report = new LinkedList<>();

        try {
          result.put(bpid, volume.compileReport(bpid, report, this)); // TODO: GABRIEL - report empty for provided blocks, is this correct (probably yes)?
        } catch (InterruptedException ex) {
          // Exit quickly and flag the scanner to do the same
          result = null;
          break;
        }
      }
      return result;
    }
  }
}
