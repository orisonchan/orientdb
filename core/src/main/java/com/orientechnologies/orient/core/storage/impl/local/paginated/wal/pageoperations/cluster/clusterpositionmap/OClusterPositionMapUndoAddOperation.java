package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.cluster.clusterpositionmap;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.cluster.OClusterPositionMapBucket;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;

import java.nio.ByteBuffer;

public final class OClusterPositionMapUndoAddOperation extends OClusterPositionMapPageOperation {
  private int oldPageIndex;
  private int oldRecordPosition;

  public OClusterPositionMapUndoAddOperation() {
  }

  public OClusterPositionMapUndoAddOperation(final int oldPageIndex, final int oldRecordPosition) {
    super();
    this.oldPageIndex = oldPageIndex;
    this.oldRecordPosition = oldRecordPosition;
  }

  int getOldPageIndex() {
    return oldPageIndex;
  }

  public int getOldRecordPosition() {
    return oldRecordPosition;
  }

  @Override
  protected void doUndo(final OClusterPositionMapBucket bucket) {
    bucket.add(oldPageIndex, oldRecordPosition);
  }

  @Override
  public byte getId() {
    return WALRecordTypes.CLUSTER_POSITION_MAP_UNDO_ADD;
  }

  @Override
  protected void serializeToByteBuffer(final ByteBuffer buffer) {
    buffer.putInt(oldPageIndex);
    buffer.putInt(oldRecordPosition);
  }

  @Override
  protected void deserializeFromByteBuffer(final ByteBuffer buffer) {
    oldPageIndex = buffer.getInt();
    oldRecordPosition = buffer.getInt();
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + 2 * OIntegerSerializer.INT_SIZE;
  }
}
