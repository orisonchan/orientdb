package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.cluster.clusterpositionmap;

import com.orientechnologies.orient.core.storage.cluster.OClusterPositionMapBucket;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;

import java.nio.ByteBuffer;

public final class OClusterPositionMapAddOperation extends OClusterPositionMapPageOperation {
  public OClusterPositionMapAddOperation() {
  }

  @Override
  protected void doUndo(final OClusterPositionMapBucket bucket) {
    bucket.undoAdd();
  }

  @Override
  public byte getId() {
    return WALRecordTypes.CLUSTER_POSITION_MAP_ADD;
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
