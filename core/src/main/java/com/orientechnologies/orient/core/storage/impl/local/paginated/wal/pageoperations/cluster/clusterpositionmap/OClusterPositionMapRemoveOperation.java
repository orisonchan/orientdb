package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.cluster.clusterpositionmap;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.cluster.OClusterPositionMapBucket;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;

import java.nio.ByteBuffer;

public final class OClusterPositionMapRemoveOperation extends OClusterPositionMapPageOperation {
  private int index;

  public OClusterPositionMapRemoveOperation() {
  }

  public OClusterPositionMapRemoveOperation(final int index) {
    super();
    this.index = index;
  }

  public int getIndex() {
    return index;
  }

  @Override
  protected void doUndo(final OClusterPositionMapBucket bucket) {
    bucket.undoRemove(index);
  }

  @Override
  public byte getId() {
    return WALRecordTypes.CLUSTER_POSITION_MAP_REMOVE;
  }

  @Override
  protected void serializeToByteBuffer(final ByteBuffer buffer) {
    buffer.putInt(index);
  }

  @Override
  protected void deserializeFromByteBuffer(final ByteBuffer buffer) {
    index = buffer.getInt();
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + OIntegerSerializer.INT_SIZE;
  }
}
