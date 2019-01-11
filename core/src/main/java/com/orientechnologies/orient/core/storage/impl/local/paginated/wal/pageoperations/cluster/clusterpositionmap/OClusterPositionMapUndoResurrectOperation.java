package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.cluster.clusterpositionmap;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.cluster.OClusterPositionMapBucket;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;

import java.nio.ByteBuffer;

public final class OClusterPositionMapUndoResurrectOperation extends OClusterPositionMapPageOperation {
  private int index;

  private int oldRecordPageIndex;
  private int oldRecordPosition;

  public OClusterPositionMapUndoResurrectOperation() {
  }

  public OClusterPositionMapUndoResurrectOperation(final int index, final int oldRecordPageIndex, final int oldRecordPosition) {
    super();
    this.index = index;
    this.oldRecordPageIndex = oldRecordPageIndex;
    this.oldRecordPosition = oldRecordPosition;
  }

  public int getIndex() {
    return index;
  }

  int getOldRecordPageIndex() {
    return oldRecordPageIndex;
  }

  public int getOldRecordPosition() {
    return oldRecordPosition;
  }

  @Override
  public byte getId() {
    return WALRecordTypes.CLUSTER_POSITION_MAP_UNDO_RESURRECT;
  }

  @Override
  protected void doUndo(final OClusterPositionMapBucket bucket) {
    bucket.undoResurrect(index, new OClusterPositionMapBucket.PositionEntry(oldRecordPageIndex, oldRecordPosition));
  }

  @Override
  protected void serializeToByteBuffer(final ByteBuffer buffer) {
    buffer.putInt(index);

    buffer.putInt(oldRecordPageIndex);
    buffer.putInt(oldRecordPosition);
  }

  @Override
  protected void deserializeFromByteBuffer(final ByteBuffer buffer) {
    index = buffer.getInt();
    oldRecordPageIndex = buffer.getInt();
    oldRecordPosition = buffer.getInt();
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + 3 * OIntegerSerializer.INT_SIZE;
  }
}
