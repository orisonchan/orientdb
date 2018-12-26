package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.cluster.clusterpage;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cluster.OClusterPage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OPageOperationRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;

import java.nio.ByteBuffer;

public final class OClusterPageSetPrevPageOperation extends OPageOperationRecord<OClusterPage> {
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
  protected OClusterPage createPageInstance(final OCacheEntry cacheEntry) {
    return new OClusterPage(cacheEntry, false);
  }

  @Override
  protected void doUndo(final OClusterPage clusterPage) {
    clusterPage.setPrevPage(oldPrevPage);
  }

  @Override
  public boolean isUpdateMasterRecord() {
    return false;
  }

  @Override
  public byte getId() {
    return WALRecordTypes.CLUSTER_PAGE_SET_PREV_PAGE;
  }

  @Override
  public int toStream(final byte[] content, int offset) {
    offset = super.toStream(content, offset);

    OIntegerSerializer.INSTANCE.serializeNative(oldPrevPage, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    return offset;
  }

  @Override
  public void toStream(final ByteBuffer buffer) {
    super.toStream(buffer);

    buffer.putInt(oldPrevPage);
  }

  @Override
  public int fromStream(final byte[] content, int offset) {
    offset = super.fromStream(content, offset);

    oldPrevPage = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    return offset;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + OIntegerSerializer.INT_SIZE;
  }
}
