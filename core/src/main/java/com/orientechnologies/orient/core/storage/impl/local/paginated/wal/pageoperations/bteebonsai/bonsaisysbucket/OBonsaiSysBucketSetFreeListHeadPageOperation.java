package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.bteebonsai.bonsaisysbucket;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.OBonsaiBucketPointer;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.OSysBucket;

import java.nio.ByteBuffer;

public final class OBonsaiSysBucketSetFreeListHeadPageOperation extends OBonsaiSysBucketPageOperation {
  private int freeListPageIndex;
  private int freeListPageOffset;

  private int prevFreeListPageIndex;
  private int prevFreeListPageOffset;

  public OBonsaiSysBucketSetFreeListHeadPageOperation() {
  }

  public OBonsaiSysBucketSetFreeListHeadPageOperation(final int freeListPageIndex, final int freeListPageOffset,
      final int prevFreeListPageIndex, final int prevFreeListPageOffset) {
    this.freeListPageIndex = freeListPageIndex;
    this.freeListPageOffset = freeListPageOffset;
    this.prevFreeListPageIndex = prevFreeListPageIndex;
    this.prevFreeListPageOffset = prevFreeListPageOffset;
  }

  @Override
  protected final void doRedo(final OSysBucket page) {
    page.setFreeListHead(new OBonsaiBucketPointer(freeListPageIndex, freeListPageOffset));
  }

  @Override
  protected final void doUndo(final OSysBucket page) {
    page.setFreeListHead(new OBonsaiBucketPointer(prevFreeListPageIndex, prevFreeListPageOffset));
  }

  @Override
  protected void serializeToByteBuffer(final ByteBuffer buffer) {
    buffer.putInt(freeListPageIndex);
    buffer.putInt(freeListPageOffset);

    buffer.putInt(prevFreeListPageIndex);
    buffer.putInt(prevFreeListPageOffset);

  }

  @Override
  protected void deserializeFromByteBuffer(final ByteBuffer buffer) {
    freeListPageIndex = buffer.getInt();
    freeListPageOffset = buffer.getInt();

    prevFreeListPageIndex = buffer.getInt();
    prevFreeListPageOffset = buffer.getInt();
  }

  @Override
  public final byte getId() {
    return WALRecordTypes.SBTREE_BONSAI_SYS_BUCKET_SET_FREE_LIST_HEAD;
  }

  @Override
  public final int serializedSize() {
    return super.serializedSize() + 4 * OIntegerSerializer.INT_SIZE;
  }
}
