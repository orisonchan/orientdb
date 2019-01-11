package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.cluster.clusterstatevone;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.cluster.v1.OPaginatedClusterStateV1;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;

import java.nio.ByteBuffer;

public final class OClusterStateVOneSetFreeListPageOperation extends OClusterStateVOnePageOperation {
  private int index;
  private int oldFreeListPage;

  public OClusterStateVOneSetFreeListPageOperation() {
  }

  public OClusterStateVOneSetFreeListPageOperation(final int index, final int oldFreeListPage) {
    this.index = index;
    this.oldFreeListPage = oldFreeListPage;
  }

  public final int getIndex() {
    return index;
  }

  final int getOldFreeListPage() {
    return oldFreeListPage;
  }

  @Override
  protected void doUndo(final OPaginatedClusterStateV1 clusterState) {
    clusterState.setFreeListPage(index, oldFreeListPage);
  }

  @Override
  public final byte getId() {
    return WALRecordTypes.CLUSTER_STATE_V_ONE_SET_FREE_LIST_PAGE;
  }

  @Override
  protected void serializeToByteBuffer(final ByteBuffer buffer) {
    buffer.putInt(index);
    buffer.putInt(oldFreeListPage);
  }

  @Override
  protected void deserializeFromByteBuffer(final ByteBuffer buffer) {
    index = buffer.getInt();
    oldFreeListPage = buffer.getInt();
  }

  @Override
  public final int serializedSize() {
    return super.serializedSize() + 2 * OIntegerSerializer.INT_SIZE;
  }
}
