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
package org.apache.hadoop.hdfs.server.mover;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.balancer.Dispatcher.DBlock;
import org.apache.hadoop.hdfs.server.balancer.NameNodeConnector;
import org.apache.hadoop.hdfs.server.mover.Mover.MLocation;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.GenericOptionsParser;
import org.junit.Assert;
import org.junit.Test;

public class TestMover {
  static Mover newMover(Configuration conf) throws IOException, URISyntaxException {
    final Collection<URI> namenodes = DFSUtil.getNsServiceRpcUris(conf);
    Assert.assertEquals(1, namenodes.size());

    final List<NameNodeConnector> nncs = NameNodeConnector.newNameNodeConnectors(
        namenodes, Mover.class.getSimpleName(), Mover.MOVER_ID_PATH, conf);
    return new Mover(nncs.get(0), conf);
  }

  @Test
  public void testScheduleSameBlock() throws IOException, URISyntaxException {
    final Configuration conf = new HdfsConfiguration();
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(4).build();
    try {
      cluster.waitActive();
      final DistributedFileSystem dfs = cluster.getFileSystem();
      final String file = "/testScheduleSameBlock/file";
      
      {
        final FSDataOutputStream out = dfs.create(new Path(file));
        out.writeChars("testScheduleSameBlock");
        out.close();
      }

      final Mover mover = newMover(conf);
      mover.init();
      final Mover.Processor processor = mover.new Processor();

      final LocatedBlock lb = dfs.getClient().getLocatedBlocks(file, 0).get(0);
      final List<MLocation> locations = MLocation.toLocations(lb);
      final MLocation ml = locations.get(0);
      final DBlock db = mover.newDBlock(lb.getBlock().getLocalBlock(), locations);

      final List<StorageType> storageTypes = new ArrayList<StorageType>(
          Arrays.asList(StorageType.DEFAULT, StorageType.DEFAULT));
      Assert.assertTrue(processor.scheduleMoveReplica(db, ml, storageTypes));
      Assert.assertFalse(processor.scheduleMoveReplica(db, ml, storageTypes));
    } finally {
      cluster.shutdown();
    }
  }

  private void checkMovePaths(List<Path> actual, Path... expected) {
    Assert.assertEquals(expected.length, actual.size());
    for (Path p : expected) {
      Assert.assertTrue(actual.contains(p));
    }
  }

  /**
   * Test Mover Cli by specifying a list of files/directories using option "-p".
   * There is only one namenode (and hence name service) specified in the conf.
   */
  @Test
  public void testMoverCli() throws Exception {
    final MiniDFSCluster cluster = new MiniDFSCluster
        .Builder(new HdfsConfiguration()).numDataNodes(0).build();
    try {
      final Configuration conf = cluster.getConfiguration(0);
      try {
        Mover.Cli.getNameNodePathsToMove(conf, "-p", "/foo", "bar");
        Assert.fail("Expected exception for illegal path bar");
      } catch (IllegalArgumentException e) {
        GenericTestUtils.assertExceptionContains("bar is not absolute", e);
      }

      Map<URI, List<Path>> movePaths = Mover.Cli.getNameNodePathsToMove(conf);
      Collection<URI> namenodes = DFSUtil.getNameNodesRPCAddressesAsURIs(conf);
      Assert.assertEquals(1, namenodes.size());
      Assert.assertEquals(1, movePaths.size());
      URI nn = namenodes.iterator().next();
      Assert.assertTrue(movePaths.containsKey(nn));
      Assert.assertNull(movePaths.get(nn));

      movePaths = Mover.Cli.getNameNodePathsToMove(conf, "-p", "/foo", "/bar");
      namenodes = DFSUtil.getNameNodesRPCAddressesAsURIs(conf);
      Assert.assertEquals(1, movePaths.size());
      nn = namenodes.iterator().next();
      Assert.assertTrue(movePaths.containsKey(nn));
      checkMovePaths(movePaths.get(nn), new Path("/foo"), new Path("/bar"));
    } finally {
      cluster.shutdown();
    }
  }

}
