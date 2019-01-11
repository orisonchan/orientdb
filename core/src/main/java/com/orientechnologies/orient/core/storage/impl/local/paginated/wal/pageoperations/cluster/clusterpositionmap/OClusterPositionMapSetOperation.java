package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.cluster.clusterpositionmap;

import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.cluster.OClusterPositionMapBucket;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;

import java.nio.ByteBuffer;

public final class OClusterPositionMapSetOperation extends OClusterPositionMapPageOperation {
  private int index;

  private int oldRecordPageIndex;
  private int oldRecordPosition;

  private byte oldFlag;

  public OClusterPositionMapSetOperation() {
  }

  public OClusterPositionMapSetOperation(final int index, final int oldRecordPageIndex, final int oldRecordPosition,
      final byte oldFlag) {
    super();
    this.index = index;

    this.oldRecordPageIndex = oldRecordPageIndex;
    this.oldRecordPosition = oldRecordPosition;

    this.oldFlag = oldFlag;
  }

  public final int getIndex() {
    return index;
  }

  final int getOldRecordPageIndex() {
    return oldRecordPageIndex;
  }

  public final int getOldRecordPosition() {
    return oldRecordPosition;
  }

  final byte getOldFlag() {
    return oldFlag;
  }

  @Override
  protected void doUndo(final OClusterPositionMapBucket bucket) {
    bucket.undoSet(index, oldFlag, new OClusterPositionMapBucket.PositionEntry(oldRecordPageIndex, oldRecordPosition));
  }

  @Override
  public final byte getId() {
    return WALRecordTypes.CLUSTER_POSITION_MAP_SET;
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
  public final int serializedSize() {
    return super.serializedSize() + 3 * OIntegerSerializer.INT_SIZE + OByteSerializer.BYTE_SIZE;
  }
}
