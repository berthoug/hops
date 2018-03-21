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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathHandle;
import org.apache.hadoop.hdfs.StorageType;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.common.FileRegion;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;

import java.io.File;
import java.net.URI;

/**
 * This class is to be used as a builder for {@link ReplicaInfo} objects.
 * The state of the replica is used to determine which object is instantiated.
 */
public class ReplicaBuilder {

  private ReplicaState state;
  private long blockId;
  private long genStamp;
  private long length;
  private FsVolumeSpi volume;
  private File directoryUsed;
  private long bytesToReserve;
  private Thread writer;
  private long recoveryId;
  private Block block;

  private ReplicaInfo fromReplica;

  private URI uri;
  private long offset;
  private Configuration conf;
  private FileRegion fileRegion;
  private FileSystem remoteFS;
  private PathHandle pathHandle;
  private String pathSuffix;
  private Path pathPrefix;

  public ReplicaBuilder(ReplicaState state) {
    volume = null;
    writer = null;
    block = null;
    length = -1;
    fileRegion = null;
    conf = null;
    fromReplica = null;
    uri = null;
    this.state = state;
    pathHandle = null;
  }

  public ReplicaBuilder setState(ReplicaState state) {
    this.state = state;
    return this;
  }

  public ReplicaBuilder setBlockId(long blockId) {
    this.blockId = blockId;
    return this;
  }

  public ReplicaBuilder setGenerationStamp(long genStamp) {
    this.genStamp = genStamp;
    return this;
  }

  public ReplicaBuilder setLength(long length) {
    this.length = length;
    return this;
  }

  public ReplicaBuilder setFsVolume(FsVolumeSpi volume) {
    this.volume = volume;
    return this;
  }

  public ReplicaBuilder setDirectoryToUse(File dir) {
    this.directoryUsed = dir;
    return this;
  }

  public ReplicaBuilder setBytesToReserve(long bytesToReserve) {
    this.bytesToReserve = bytesToReserve;
    return this;
  }

  public ReplicaBuilder setWriterThread(Thread writer) {
    this.writer = writer;
    return this;
  }

  public ReplicaBuilder from(ReplicaInfo fromReplica) {
    this.fromReplica = fromReplica;
    return this;
  }

  public ReplicaBuilder setRecoveryId(long recoveryId) {
    this.recoveryId = recoveryId;
    return this;
  }

  public ReplicaBuilder setBlock(Block block) {
    this.block = block;
    return this;
  }

  public ReplicaBuilder setURI(URI uri) {
    this.uri = uri;
    return this;
  }

  public ReplicaBuilder setConf(Configuration conf) {
    this.conf = conf;
    return this;
  }

  public ReplicaBuilder setOffset(long offset) {
    this.offset = offset;
    return this;
  }

  public ReplicaBuilder setFileRegion(FileRegion fileRegion) {
    this.fileRegion = fileRegion;
    return this;
  }

  public ReplicaBuilder setRemoteFS(FileSystem remoteFS) {
    this.remoteFS = remoteFS;
    return this;
  }

  /**
   * Set the suffix of the {@link Path} associated with the replica.
   * Intended to be use only for {@link ProvidedReplica}s.
   * @param suffix the path suffix.
   * @return the builder with the path suffix set.
   */
  public ReplicaBuilder setPathSuffix(String suffix) {
    this.pathSuffix = suffix;
    return this;
  }

  /**
   * Set the prefix of the {@link Path} associated with the replica.
   * Intended to be use only for {@link ProvidedReplica}s.
   * @param prefix the path prefix.
   * @return the builder with the path prefix set.
   */
  public ReplicaBuilder setPathPrefix(Path prefix) {
    this.pathPrefix = prefix;
    return this;
  }

  public ReplicaBuilder setPathHandle(PathHandle pathHandle) {
    this.pathHandle = pathHandle;
    return this;
  }

  private ProvidedReplica buildProvidedFinalizedReplica()
          throws IllegalArgumentException {
    ProvidedReplica info = null;
    if (fromReplica != null) {
      throw new IllegalArgumentException("Finalized PROVIDED replica " +
              "cannot be constructed from another replica");
    }
    if (fileRegion == null && uri == null &&
            (pathPrefix == null || pathSuffix == null)) {
      throw new IllegalArgumentException(
              "Trying to construct a provided replica on " + volume +
                      " without enough information");
    }
    if (fileRegion == null) {
      if (uri != null) {
        info = new FinalizedProvidedReplica(blockId, uri, offset,
                length, genStamp, pathHandle, volume, conf, remoteFS);
      } else {
        info = new FinalizedProvidedReplica(blockId, pathPrefix, pathSuffix,
                offset, length, genStamp, pathHandle, volume, conf, remoteFS);
      }
    } else {
      info = new FinalizedProvidedReplica(fileRegion, volume, conf, remoteFS);
    }
    return info;
  }

  private ProvidedReplica buildProvidedReplica()
          throws IllegalArgumentException {
    ProvidedReplica info = null;
    switch(this.state) {
      case FINALIZED:
        info = buildProvidedFinalizedReplica();
        break;
      case RWR:
      case RUR:
      case RBW:
      case TEMPORARY:
      default:
        throw new IllegalArgumentException("Unknown replica state " +
                state + " for PROVIDED replica");
    }
    return info;
  }

  public ReplicaInfo build() throws IllegalArgumentException {

    ReplicaInfo info = null;
    if(volume != null && volume.getStorageType() == StorageType.PROVIDED) {
      info = buildProvidedReplica();
    }

    return info;
  }
}
