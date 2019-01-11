package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.cluster.clusterpage;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.cluster.OClusterPage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;

import java.nio.ByteBuffer;

public final class OClusterPageSetPrevPageOperation extends OClusterPageOperation {
  private int oldPrevPage;

  public OClusterPageSetPrevPageOperation() {
  }

  public OClusterPageSetPrevPageOperation(final int oldPrevPage) {
    this.oldPrevPage = oldPrevPage;
  }

  int getOldPrevPage() {
    return oldPrevPage;
  }

  @Override
  protected void doUndo(final OClusterPage clusterPage) {
    clusterPage.setPrevPage(oldPrevPage);
  }

  @Override
  public byte getId() {
    return WALRecordTypes.CLUSTER_PAGE_SET_PREV_PAGE;
  }

  @Override
  protected void serializeToByteBuffer(final ByteBuffer buffer) {
    buffer.putInt(oldPrevPage);
  }

  @Override
  protected void deserializeFromByteBuffer(final ByteBuffer buffer) {
    oldPrevPage = buffer.getInt();
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + OIntegerSerializer.INT_SIZE;
  }
}
