package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.cluster.clusterpage;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.cluster.OClusterPage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;

import java.nio.ByteBuffer;

public final class OClusterPageAppendRecordOperation extends OClusterPageOperation {
  private int index;

  public OClusterPageAppendRecordOperation() {
  }

  public OClusterPageAppendRecordOperation(final int index) {
    this.index = index;
  }

  @Override
  protected void doUndo(final OClusterPage clusterPage) {
    clusterPage.deleteRecord(index);
  }

  public int getIndex() {
    return index;
  }

  @Override
  public byte getId() {
    return WALRecordTypes.CLUSTER_PAGE_APPEND_RECORD;
  }

  @Override
  protected void serializeToByteBuffer(final ByteBuffer buffer) {
    buffer.putInt(index);
  }

  @Override
  protected void deserializeFromByteBuffer(final ByteBuffer buffer) {
    index = buffer.getInt();
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + OIntegerSerializer.INT_SIZE;
  }
}
