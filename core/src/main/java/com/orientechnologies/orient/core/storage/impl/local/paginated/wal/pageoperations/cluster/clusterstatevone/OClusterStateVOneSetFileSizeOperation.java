package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.cluster.clusterstatevone;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.cluster.v1.OPaginatedClusterStateV1;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;

import java.nio.ByteBuffer;

public final class OClusterStateVOneSetFileSizeOperation extends OClusterStateVOnePageOperation {
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
  protected void doUndo(final OPaginatedClusterStateV1 clusterState) {
    clusterState.setFileSize(oldFileSize);
  }

  @Override
  public final byte getId() {
    return WALRecordTypes.CLUSTER_STATE_V_ONE_SET_FILE_SIZE;
  }

  @Override
  protected void serializeToByteBuffer(final ByteBuffer buffer) {
    buffer.putInt(oldFileSize);
  }

  @Override
  protected void deserializeFromByteBuffer(final ByteBuffer buffer) {
    oldFileSize = buffer.getInt();
  }

  @Override
  public final int serializedSize() {
    return super.serializedSize() + OIntegerSerializer.INT_SIZE;
  }
}
