package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.cluster.clusterstatevone;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cluster.v1.OPaginatedClusterStateV1;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OPageOperationRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;

import java.nio.ByteBuffer;

public class OClusterStateVOneSetFreeListPageOperation extends OPageOperationRecord {
  private int index;
  private int freeListPage;
  private int oldFreeListPage;

  public OClusterStateVOneSetFreeListPageOperation() {
  }

  public OClusterStateVOneSetFreeListPageOperation(int index, int freeListPage, int oldFreeListPage) {
    this.index = index;
    this.freeListPage = freeListPage;
    this.oldFreeListPage = oldFreeListPage;
  }

  public int getIndex() {
    return index;
  }

  public int getFreeListPage() {
    return freeListPage;
  }

  public int getOldFreeListPage() {
    return oldFreeListPage;
  }

  @Override
  protected void doRedo(OCacheEntry cacheEntry) {
    final OPaginatedClusterStateV1 clusterState = new OPaginatedClusterStateV1(cacheEntry, false);
    clusterState.setFreeListPage(index, freeListPage);
  }

  @Override
  protected void doUndo(OCacheEntry cacheEntry) {
    final OPaginatedClusterStateV1 clusterState = new OPaginatedClusterStateV1(cacheEntry, false);
    clusterState.setFreeListPage(index, oldFreeListPage);
  }

  @Override
  public boolean isUpdateMasterRecord() {
    return false;
  }

  @Override
  public byte getId() {
    return WALRecordTypes.CLUSTER_STATE_V_ONE_SET_FREE_LIST_PAGE;
  }

  @Override
  public int toStream(byte[] content, int offset) {
    offset = super.toStream(content, offset);

    OIntegerSerializer.INSTANCE.serializeNative(index, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    OIntegerSerializer.INSTANCE.serializeNative(freeListPage, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    OIntegerSerializer.INSTANCE.serializeNative(oldFreeListPage, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    return offset;
  }

  @Override
  public void toStream(ByteBuffer buffer) {
    super.toStream(buffer);

    buffer.putInt(index);
    buffer.putInt(freeListPage);
    buffer.putInt(oldFreeListPage);
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    offset = super.fromStream(content, offset);

    index = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    freeListPage = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    oldFreeListPage = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    return offset;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + 3 * OIntegerSerializer.INT_SIZE;
  }
}
