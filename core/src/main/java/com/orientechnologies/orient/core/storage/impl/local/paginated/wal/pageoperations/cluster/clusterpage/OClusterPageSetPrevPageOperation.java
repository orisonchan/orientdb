package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.cluster.clusterpage;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cluster.OClusterPage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OPageOperationRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;

import java.nio.ByteBuffer;

public final class OClusterPageSetPrevPageOperation extends OPageOperationRecord<OClusterPage> {
  private int prevPage;
  private int oldPrevPage;

  public OClusterPageSetPrevPageOperation() {
  }

  public OClusterPageSetPrevPageOperation(int prevPage, int oldPrevPage) {
    this.prevPage = prevPage;
    this.oldPrevPage = oldPrevPage;
  }

  int getPrevPage() {
    return prevPage;
  }

  int getOldPrevPage() {
    return oldPrevPage;
  }

  @Override
  protected OClusterPage createPageInstance(OCacheEntry cacheEntry) {
    return new OClusterPage(cacheEntry, false);
  }

  @Override
  protected void doRedo(OClusterPage clusterPage) {
    clusterPage.setPrevPage(prevPage);
  }

  @Override
  protected void doUndo(OClusterPage clusterPage) {
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
  public int toStream(byte[] content, int offset) {
    offset = super.toStream(content, offset);

    OIntegerSerializer.INSTANCE.serializeNative(prevPage, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    OIntegerSerializer.INSTANCE.serializeNative(oldPrevPage, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    return offset;
  }

  @Override
  public void toStream(ByteBuffer buffer) {
    super.toStream(buffer);

    buffer.putInt(prevPage);
    buffer.putInt(oldPrevPage);
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    offset = super.fromStream(content, offset);

    prevPage = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    oldPrevPage = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    return offset;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + 2 * OIntegerSerializer.INT_SIZE;
  }
}
