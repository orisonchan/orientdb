package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.cluster.clusterpositionmap;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cluster.OClusterPositionMapBucket;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OPageOperationRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;

import java.nio.ByteBuffer;

public final class OClusterPositionMapUndoRemove extends OPageOperationRecord<OClusterPositionMapBucket> {
  private int index;
  private int recordPageIndex;
  private int recordPosition;

  public OClusterPositionMapUndoRemove() {
  }

  public OClusterPositionMapUndoRemove(int index, int pageIndex, int recordPosition) {
    super();
    this.index = index;

    this.recordPageIndex = pageIndex;
    this.recordPosition = recordPosition;
  }

  public int getIndex() {
    return index;
  }

  public int getRecordPageIndex() {
    return recordPageIndex;
  }

  public int getRecordPosition() {
    return recordPosition;
  }

  @Override
  protected OClusterPositionMapBucket createPageInstance(OCacheEntry cacheEntry) {
    return new OClusterPositionMapBucket(cacheEntry, false);
  }

  @Override
  protected void doRedo(OClusterPositionMapBucket bucket) {
    bucket.undoRemove(index);
  }

  @Override
  protected void doUndo(OClusterPositionMapBucket bucket) {
    if (bucket.getSize() == index) {
      bucket.add(recordPageIndex, recordPosition);
    } else {
      bucket.undoSet(index, OClusterPositionMapBucket.FILLED,
          new OClusterPositionMapBucket.PositionEntry(recordPageIndex, recordPosition));
    }
  }

  @Override
  public boolean isUpdateMasterRecord() {
    return false;
  }

  @Override
  public byte getId() {
    return WALRecordTypes.CLUSTER_POSITION_MAP_UNDO_REMOVE;
  }

  @Override
  public int toStream(byte[] content, int offset) {
    offset = super.toStream(content, offset);

    OIntegerSerializer.INSTANCE.serializeNative(index, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    OIntegerSerializer.INSTANCE.serializeNative(recordPageIndex, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    OIntegerSerializer.INSTANCE.serializeNative(recordPosition, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    return offset;
  }

  @Override
  public void toStream(ByteBuffer buffer) {
    super.toStream(buffer);

    buffer.putInt(index);
    buffer.putInt(recordPageIndex);
    buffer.putInt(recordPosition);
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    offset = super.fromStream(content, offset);

    index = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    recordPageIndex = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    recordPosition = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    return offset;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + 3 * OIntegerSerializer.INT_SIZE;
  }
}
