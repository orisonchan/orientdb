package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.cluster.clusterpositionmap;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cluster.OClusterPositionMapBucket;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OPageOperationRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;

import java.nio.ByteBuffer;

public final class OClusterPositionMapAddOperation extends OPageOperationRecord<OClusterPositionMapBucket> {
  private int recordPageIndex;
  private int recordPosition;

  public OClusterPositionMapAddOperation() {
  }

  public OClusterPositionMapAddOperation(int recordPageIndex, int recordPosition) {
    super();
    this.recordPageIndex = recordPageIndex;
    this.recordPosition = recordPosition;
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
    bucket.add(recordPageIndex, recordPosition);
  }

  @Override
  protected void doUndo(OClusterPositionMapBucket bucket) {
    bucket.undoAdd();
  }

  @Override
  public boolean isUpdateMasterRecord() {
    return false;
  }

  @Override
  public byte getId() {
    return WALRecordTypes.CLUSTER_POSITION_MAP_ADD;
  }

  @Override
  public int toStream(byte[] content, int offset) {
    offset = super.toStream(content, offset);

    OIntegerSerializer.INSTANCE.serializeNative(recordPageIndex, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    OIntegerSerializer.INSTANCE.serializeNative(recordPosition, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    return offset;
  }

  @Override
  public void toStream(ByteBuffer buffer) {
    super.toStream(buffer);

    buffer.putInt(recordPageIndex);
    buffer.putInt(recordPosition);
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    offset = super.fromStream(content, offset);

    recordPageIndex = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    recordPosition = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    return offset;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + 2 * OIntegerSerializer.INT_SIZE;
  }
}
