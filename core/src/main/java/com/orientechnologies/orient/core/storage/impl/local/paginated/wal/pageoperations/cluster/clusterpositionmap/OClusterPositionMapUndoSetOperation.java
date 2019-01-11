package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.cluster.clusterpositionmap;

import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.cluster.OClusterPositionMapBucket;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;

import java.nio.ByteBuffer;

public final class OClusterPositionMapUndoSetOperation extends OClusterPositionMapPageOperation {
  private int index;

  private int  oldRecordPageIndex;
  private int  oldRecordPosition;
  private byte oldFlag;

  public OClusterPositionMapUndoSetOperation() {
  }

  public OClusterPositionMapUndoSetOperation(final int index, final int oldRecordPageIndex, final int oldRecordPosition,
      final byte oldFlag) {
    super();

    this.index = index;
    this.oldRecordPageIndex = oldRecordPageIndex;
    this.oldRecordPosition = oldRecordPosition;
    this.oldFlag = oldFlag;
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

  byte getOldFlag() {
    return oldFlag;
  }

  @Override
  protected void doUndo(final OClusterPositionMapBucket bucket) {
    bucket.undoSet(index, oldFlag, new OClusterPositionMapBucket.PositionEntry(oldRecordPageIndex, oldRecordPosition));
  }

  @Override
  public byte getId() {
    return WALRecordTypes.CLUSTER_POSITION_MAP_UNDO_SET;
  }

  @Override
  protected void serializeToByteBuffer(final ByteBuffer buffer) {
    buffer.putInt(index);

    buffer.putInt(oldRecordPageIndex);
    buffer.putInt(oldRecordPosition);

    buffer.put(oldFlag);
  }

  @Override
  protected void deserializeFromByteBuffer(final ByteBuffer buffer) {
    index = buffer.getInt();
    oldRecordPageIndex = buffer.getInt();
    oldRecordPosition = buffer.getInt();

    oldFlag = buffer.get();
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + 3 * OIntegerSerializer.INT_SIZE + OByteSerializer.BYTE_SIZE;
  }
}
