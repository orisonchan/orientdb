package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.cluster.clusterpositionmap;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cluster.OClusterPositionMapBucket;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OPageOperationRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;

import java.nio.ByteBuffer;

public final class OClusterPositionMapRemoveOperation extends OPageOperationRecord<OClusterPositionMapBucket> {
  private int index;

  public OClusterPositionMapRemoveOperation() {
  }

  public OClusterPositionMapRemoveOperation(int index) {
    super();
    this.index = index;
  }

  public int getIndex() {
    return index;
  }

  @Override
  protected OClusterPositionMapBucket createPageInstance(OCacheEntry cacheEntry) {
    return new OClusterPositionMapBucket(cacheEntry, false);
  }

  @Override
  protected void doRedo(OClusterPositionMapBucket bucket) {
    bucket.remove(index);
  }

  @Override
  protected void doUndo(OClusterPositionMapBucket bucket) {
    bucket.undoRemove(index);
  }

  @Override
  public boolean isUpdateMasterRecord() {
    return false;
  }

  @Override
  public byte getId() {
    return WALRecordTypes.CLUSTER_POSITION_MAP_REMOVE;
  }

  @Override
  public int toStream(byte[] content, int offset) {
    offset = super.toStream(content, offset);

    OIntegerSerializer.INSTANCE.serializeNative(index, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    return offset;
  }

  @Override
  public void toStream(ByteBuffer buffer) {
    super.toStream(buffer);

    buffer.putInt(index);
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    offset = super.fromStream(content, offset);

    index = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    return offset;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + OIntegerSerializer.INT_SIZE;
  }
}
