package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.bteebonsai.bonsaibucket;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.OBonsaiBucketPointer;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.OSBTreeBonsaiBucket;

import java.nio.ByteBuffer;

public final class OBonsaiBucketSetFreeListPointerPageOperation extends OBonsaiBucketPageOperation {
  private int prevPageIndex;
  private int prevPageOffset;

  public OBonsaiBucketSetFreeListPointerPageOperation() {
  }

  public OBonsaiBucketSetFreeListPointerPageOperation(final int pageOffset, final int prevPageIndex, final int prevPageOffset) {
    super(pageOffset);
    this.prevPageIndex = prevPageIndex;
    this.prevPageOffset = prevPageOffset;
  }

  int getPrevPageIndex() {
    return prevPageIndex;
  }

  int getPrevPageOffset() {
    return prevPageOffset;
  }

  @Override
  protected final void doUndo(final OSBTreeBonsaiBucket page) {
    page.setFreeListPointer(new OBonsaiBucketPointer(prevPageIndex, prevPageOffset));
  }

  @Override
  protected void serializeToByteBuffer(final ByteBuffer buffer) {
    super.serializeToByteBuffer(buffer);

    buffer.putInt(prevPageIndex);
    buffer.putInt(prevPageOffset);
  }

  @Override
  protected void deserializeFromByteBuffer(final ByteBuffer buffer) {
    super.deserializeFromByteBuffer(buffer);

    prevPageIndex = buffer.getInt();
    prevPageOffset = buffer.getInt();
  }

  @Override
  public final byte getId() {
    return WALRecordTypes.SBTREE_BONSAI_BUCKET_SET_FREE_LIST_POINTER;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + 2 * OIntegerSerializer.INT_SIZE;
  }
}
