package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.cluster.clusterstatevone;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cluster.v1.OPaginatedClusterStateV1;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OPageOperationRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;

import java.nio.ByteBuffer;

public final class OClusterStateVOneSetFileSizeOperation extends OPageOperationRecord<OPaginatedClusterStateV1> {
  private int oldFileSize;

  public OClusterStateVOneSetFileSizeOperation() {
  }

  public OClusterStateVOneSetFileSizeOperation(final int oldFileSize) {
    this.oldFileSize = oldFileSize;
  }

  final int getOldFileSize() {
    return oldFileSize;
  }

  @Override
  protected OPaginatedClusterStateV1 createPageInstance(final OCacheEntry cacheEntry) {
    return new OPaginatedClusterStateV1(cacheEntry);
  }

  @Override
  protected void doUndo(final OPaginatedClusterStateV1 clusterState) {
    clusterState.setFileSize(oldFileSize);
  }

  @Override
  public final boolean isUpdateMasterRecord() {
    return false;
  }

  @Override
  public final byte getId() {
    return WALRecordTypes.CLUSTER_STATE_V_ONE_SET_FILE_SIZE;
  }

  @Override
  public final int toStream(final byte[] content, int offset) {
    offset = super.toStream(content, offset);

    OIntegerSerializer.INSTANCE.serializeNative(oldFileSize, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    return offset;
  }

  @Override
  public final void toStream(final ByteBuffer buffer) {
    super.toStream(buffer);

    buffer.putInt(oldFileSize);
  }

  @Override
  public final int fromStream(final byte[] content, int offset) {
    offset = super.fromStream(content, offset);

    oldFileSize = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    return offset;
  }

  @Override
  public final int serializedSize() {
    return super.serializedSize() + OIntegerSerializer.INT_SIZE;
  }
}
