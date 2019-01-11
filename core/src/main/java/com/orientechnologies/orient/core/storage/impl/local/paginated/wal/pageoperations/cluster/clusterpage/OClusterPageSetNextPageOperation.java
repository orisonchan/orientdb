package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.cluster.clusterpage;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.cluster.OClusterPage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;

import java.nio.ByteBuffer;

public final class OClusterPageSetNextPageOperation extends OClusterPageOperation {
  private int oldNextPage;

  public OClusterPageSetNextPageOperation() {
  }

  public OClusterPageSetNextPageOperation(final int oldNextPage) {
    this.oldNextPage = oldNextPage;
  }

  int getOldNextPage() {
    return oldNextPage;
  }

  @Override
  protected void doUndo(final OClusterPage clusterPage) {
    clusterPage.setNextPage(oldNextPage);
  }

  @Override
  public byte getId() {
    return WALRecordTypes.CLUSTER_PAGE_SET_NEXT_PAGE;
  }

  @Override
  protected void serializeToByteBuffer(final ByteBuffer buffer) {
    buffer.putInt(oldNextPage);
  }

  @Override
  protected void deserializeFromByteBuffer(final ByteBuffer buffer) {
    oldNextPage = buffer.getInt();
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + OIntegerSerializer.INT_SIZE;
  }
}
