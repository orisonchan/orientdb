package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.bteebonsai.bonsaisysbucket;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.OBonsaiBucketPointer;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.OSysBucket;

import java.nio.ByteBuffer;

public final class OBonsaiSysBucketSetFreeListHeadPageOperation extends OBonsaiSysBucketPageOperation {
  private int prevFreeListPageIndex;
  private int prevFreeListPageOffset;

  public OBonsaiSysBucketSetFreeListHeadPageOperation() {
  }

  public OBonsaiSysBucketSetFreeListHeadPageOperation(final int prevFreeListPageIndex, final int prevFreeListPageOffset) {
    this.prevFreeListPageIndex = prevFreeListPageIndex;
    this.prevFreeListPageOffset = prevFreeListPageOffset;
  }

  int getPrevFreeListPageIndex() {
    return prevFreeListPageIndex;
  }

  int getPrevFreeListPageOffset() {
    return prevFreeListPageOffset;
  }

  @Override
  protected final void doUndo(final OSysBucket page) {
    page.setFreeListHead(new OBonsaiBucketPointer(prevFreeListPageIndex, prevFreeListPageOffset));
  }

  @Override
  protected void serializeToByteBuffer(final ByteBuffer buffer) {
    buffer.putInt(prevFreeListPageIndex);
    buffer.putInt(prevFreeListPageOffset);
  }

  @Override
  protected void deserializeFromByteBuffer(final ByteBuffer buffer) {
    prevFreeListPageIndex = buffer.getInt();
    prevFreeListPageOffset = buffer.getInt();
  }

  @Override
  public final byte getId() {
    return WALRecordTypes.SBTREE_BONSAI_SYS_BUCKET_SET_FREE_LIST_HEAD;
  }

  @Override
  public final int serializedSize() {
    return super.serializedSize() + 2 * OIntegerSerializer.INT_SIZE;
  }
}
