package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.cluster.clusterpositionmap;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.cluster.OClusterPositionMapBucket;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;

import java.nio.ByteBuffer;

public final class OClusterPositionMapUndoRemove extends OClusterPositionMapPageOperation {
  private int index;
  private int recordPageIndex;
  private int recordPosition;

  public OClusterPositionMapUndoRemove() {
  }

  public OClusterPositionMapUndoRemove(final int index, final int pageIndex, final int recordPosition) {
    super();
    this.index = index;

    this.recordPageIndex = pageIndex;
    this.recordPosition = recordPosition;
  }

  public int getIndex() {
    return index;
  }

  int getRecordPageIndex() {
    return recordPageIndex;
  }

  public int getRecordPosition() {
    return recordPosition;
  }

  @Override
  protected void doUndo(final OClusterPositionMapBucket bucket) {
    if (bucket.getSize() == index) {
      bucket.add(recordPageIndex, recordPosition);
    } else {
      bucket.undoSet(index, OClusterPositionMapBucket.FILLED,
          new OClusterPositionMapBucket.PositionEntry(recordPageIndex, recordPosition));
    }
  }

  @Override
  public byte getId() {
    return WALRecordTypes.CLUSTER_POSITION_MAP_UNDO_REMOVE;
  }

  @Override
  protected void serializeToByteBuffer(final ByteBuffer buffer) {
    buffer.putInt(index);
    buffer.putInt(recordPageIndex);
    buffer.putInt(recordPosition);
  }

  @Override
  protected void deserializeFromByteBuffer(final ByteBuffer buffer) {
    index = buffer.getInt();
    recordPageIndex = buffer.getInt();
    recordPosition = buffer.getInt();
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + 3 * OIntegerSerializer.INT_SIZE;
  }
}
