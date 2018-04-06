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
package org.apache.hadoop.hdfs.protocolPB;

import com.google.protobuf.ByteString;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ProvidedStorageLocation;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;

/**
 * Utilities for converting protobuf classes to and from hdfs-client side
 * implementation classes and other helper utilities to help in dealing with
 * protobuf.
 *
 * Note that when converting from an internal type to protobuf type, the
 * converter never return null for protobuf type. The check for internal type
 * being null must be done before calling the convert() method.
 */
public class PBHelperClient {

  private PBHelperClient() {
    /** Hidden constructor */
  }

  public static ByteString getByteString(byte[] bytes) {
    // return singleton to reduce object allocation
    return (bytes.length == 0) ? ByteString.EMPTY : ByteString.copyFrom(bytes);
  }

  // Block
  public static HdfsProtos.BlockProto convert(Block b) {
    return HdfsProtos.BlockProto.newBuilder().setBlockId(b.getBlockId())
            .setGenStamp(b.getGenerationStamp()).setNumBytes(b.getNumBytes())
            .build();
  }

  public static Block convert(HdfsProtos.BlockProto b) {
    return new Block(b.getBlockId(), b.getNumBytes(), b.getGenStamp());
  }

  public static ProvidedStorageLocation convert(
          HdfsProtos.ProvidedStorageLocationProto providedStorageLocationProto) {
    if (providedStorageLocationProto == null) {
      return null;
    }
    String path = providedStorageLocationProto.getPath();
    long length = providedStorageLocationProto.getLength();
    long offset = providedStorageLocationProto.getOffset();
    ByteString nonce = providedStorageLocationProto.getNonce();
    if (path == null || length == -1 || offset == -1 || nonce == null) {
      return null;
    } else {
      return new ProvidedStorageLocation(new Path(path), offset, length,
              nonce.toByteArray());
    }
  }

  public static HdfsProtos.ProvidedStorageLocationProto convert(
          ProvidedStorageLocation providedStorageLocation) {
    String path = providedStorageLocation.getPath().toString();
    return HdfsProtos.ProvidedStorageLocationProto.newBuilder()
            .setPath(path)
            .setLength(providedStorageLocation.getLength())
            .setOffset(providedStorageLocation.getOffset())
            .setNonce(ByteString.copyFrom(providedStorageLocation.getNonce()))
            .build();
  }


}