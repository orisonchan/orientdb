package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.cluster.clusterpositionmap;

import com.orientechnologies.orient.core.storage.cluster.OClusterPositionMapBucket;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;

import java.nio.ByteBuffer;

public final class OClusterPositionMapAllocateOperation extends OClusterPositionMapPageOperation {
  @Override
  protected void doUndo(final OClusterPositionMapBucket bucket) {
    bucket.undoAllocation();
  }

  @Override
  public byte getId() {
    return WALRecordTypes.CLUSTER_POSITION_MAP_ALLOCATE;
  }

  @Override
  protected void serializeToByteBuffer(final ByteBuffer buffer) {
    //do nothing
  }

  @Override
  protected void deserializeFromByteBuffer(final ByteBuffer buffer) {
    //do nothing
  }
}
