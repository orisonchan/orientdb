package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.cluster.clusterstatevone;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.cluster.v1.OPaginatedClusterStateV1;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;

import java.nio.ByteBuffer;

public final class OClusterStateVOneSetSizeOperation extends OClusterStateVOnePageOperation {
  private int oldSize;

  public OClusterStateVOneSetSizeOperation() {
  }

  public OClusterStateVOneSetSizeOperation(final int oldSize) {
    this.oldSize = oldSize;
  }

  int getOldSize() {
    return oldSize;
  }

  @Override
  protected void doUndo(final OPaginatedClusterStateV1 clusterState) {
    clusterState.setSize(oldSize);
  }

  @Override
  public byte getId() {
    return WALRecordTypes.CLUSTER_STATE_V_ONE_SET_SIZE;
  }

  @Override
  protected void serializeToByteBuffer(final ByteBuffer buffer) {
    buffer.putInt(oldSize);
  }

  @Override
  protected void deserializeFromByteBuffer(final ByteBuffer buffer) {
    oldSize = buffer.getInt();
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + OIntegerSerializer.INT_SIZE;
  }
}
