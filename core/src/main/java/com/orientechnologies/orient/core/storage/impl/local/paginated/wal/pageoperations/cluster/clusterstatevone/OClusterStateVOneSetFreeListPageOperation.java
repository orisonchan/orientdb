package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.cluster.clusterstatevone;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cluster.v1.OPaginatedClusterStateV1;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OPageOperationRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;

import java.nio.ByteBuffer;

public final class OClusterStateVOneSetFreeListPageOperation extends OPageOperationRecord<OPaginatedClusterStateV1> {
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
  protected OPaginatedClusterStateV1 createPageInstance(final OCacheEntry cacheEntry) {
    return new OPaginatedClusterStateV1(cacheEntry);
  }

  @Override
  protected void doUndo(final OPaginatedClusterStateV1 clusterState) {
    clusterState.setFreeListPage(index, oldFreeListPage);
  }

  @Override
  public final boolean isUpdateMasterRecord() {
    return false;
  }

  @Override
  public final byte getId() {
    return WALRecordTypes.CLUSTER_STATE_V_ONE_SET_FREE_LIST_PAGE;
  }

  @Override
  public final int toStream(final byte[] content, int offset) {
    offset = super.toStream(content, offset);

    OIntegerSerializer.INSTANCE.serializeNative(index, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    OIntegerSerializer.INSTANCE.serializeNative(oldFreeListPage, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    return offset;
  }

  @Override
  public final void toStream(final ByteBuffer buffer) {
    super.toStream(buffer);

    buffer.putInt(index);
    buffer.putInt(oldFreeListPage);
  }

  @Override
  public final int fromStream(final byte[] content, int offset) {
    offset = super.fromStream(content, offset);

    index = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    oldFreeListPage = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    return offset;
  }

  @Override
  public final int serializedSize() {
    return super.serializedSize() + 2 * OIntegerSerializer.INT_SIZE;
  }
}
