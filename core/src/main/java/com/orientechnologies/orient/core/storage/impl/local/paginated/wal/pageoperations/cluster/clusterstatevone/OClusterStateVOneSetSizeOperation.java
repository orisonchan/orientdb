package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.cluster.clusterstatevone;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.OReadCache;
import com.orientechnologies.orient.core.storage.cache.OWriteCache;
import com.orientechnologies.orient.core.storage.cluster.v1.OPaginatedClusterStateV1;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OPageOperationRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;

import java.io.IOException;
import java.nio.ByteBuffer;

public final class OClusterStateVOneSetSizeOperation extends OPageOperationRecord {
  private int size;
  private int oldSize;

  public OClusterStateVOneSetSizeOperation() {
  }

  public OClusterStateVOneSetSizeOperation(int size, int oldSize) {
    this.size = size;
    this.oldSize = oldSize;
  }

  @Override
  public void redo(OReadCache readCache, OWriteCache writeCache) throws IOException {
    final OCacheEntry cacheEntry = readCache.loadForWrite(getFileId(), getPageIndex(), false, writeCache, 1, true, null);
    try {
      final OPaginatedClusterStateV1 clusterState = new OPaginatedClusterStateV1(cacheEntry, newPage);
      clusterState.setSize(size);
    } finally {
      readCache.releaseFromWrite(cacheEntry, writeCache);
    }
  }

  @Override
  public void undo(OReadCache readCache, OWriteCache writeCache) throws IOException {
    final OCacheEntry cacheEntry = readCache.loadForWrite(getFileId(), getPageIndex(), false, writeCache, 1, true, null);
    try {
      final OPaginatedClusterStateV1 clusterState = new OPaginatedClusterStateV1(cacheEntry, newPage);
      clusterState.setSize(oldSize);
    } finally {
      readCache.releaseFromWrite(cacheEntry, writeCache);
    }

  }

  @Override
  public boolean isUpdateMasterRecord() {
    return false;
  }

  @Override
  public byte getId() {
    return WALRecordTypes.CLUSTER_STATE_V_ONE_SET_SIZE;
  }

  @Override
  public int toStream(byte[] content, int offset) {
    offset = super.toStream(content, offset);

    OIntegerSerializer.INSTANCE.serializeNative(size, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    OIntegerSerializer.INSTANCE.serializeNative(oldSize, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    return offset;
  }

  @Override
  public void toStream(ByteBuffer buffer) {
    super.toStream(buffer);

    buffer.putInt(size);
    buffer.putInt(oldSize);
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    offset = super.fromStream(content, offset);

    size = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    oldSize = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    return offset;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + 2 * OIntegerSerializer.INT_SIZE;
  }
}
