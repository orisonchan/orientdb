package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.cluster.clusterpage;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cluster.OClusterPage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OPageOperationRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;

import java.nio.ByteBuffer;

public final class OClusterPageSetNextPageOperation extends OPageOperationRecord {
  private int nextPage;
  private int oldNextPage;

  public OClusterPageSetNextPageOperation() {
  }

  public OClusterPageSetNextPageOperation(int nextPage, int oldNextPage) {
    this.nextPage = nextPage;
    this.oldNextPage = oldNextPage;
  }

  public int getNextPage() {
    return nextPage;
  }

  public int getOldNextPage() {
    return oldNextPage;
  }

  @Override
  protected void doRedo(OCacheEntry cacheEntry) {
    final OClusterPage clusterPage = new OClusterPage(cacheEntry, false);
    clusterPage.setNextPage(nextPage);
  }

  @Override
  protected void doUndo(OCacheEntry cacheEntry) {
    final OClusterPage clusterPage = new OClusterPage(cacheEntry, false);
    clusterPage.setNextPage(oldNextPage);
  }

  @Override
  public boolean isUpdateMasterRecord() {
    return false;
  }

  @Override
  public byte getId() {
    return WALRecordTypes.CLUSTER_PAGE_SET_NEXT_PAGE;
  }

  @Override
  public int toStream(byte[] content, int offset) {
    offset = super.toStream(content, offset);

    OIntegerSerializer.INSTANCE.serializeNative(nextPage, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    OIntegerSerializer.INSTANCE.serializeNative(oldNextPage, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    return offset;
  }

  @Override
  public void toStream(ByteBuffer buffer) {
    super.toStream(buffer);

    buffer.putInt(nextPage);
    buffer.putInt(oldNextPage);
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    offset = super.fromStream(content, offset);

    nextPage = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    oldNextPage = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    return offset;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + 2 * OIntegerSerializer.INT_SIZE;
  }
}
