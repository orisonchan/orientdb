package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.bteebonsai.bonsaibucket;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.OBonsaiBucketPointer;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.OSBTreeBonsaiBucket;

import java.nio.ByteBuffer;

public final class OBonsaiBucketSetFreeListPointerPageOperation extends OBonsaiBucketPageOperation {
  private int pageIndex;
  private int pageOffset;

  private int prevPageIndex;
  private int prevPageOffset;

  public OBonsaiBucketSetFreeListPointerPageOperation() {
  }

  public OBonsaiBucketSetFreeListPointerPageOperation(final int pageOffset, final int pageIndex, final int pageOffset1,
      final int prevPageIndex, final int prevPageOffset) {
    super(pageOffset);
    this.pageIndex = pageIndex;
    this.pageOffset = pageOffset1;
    this.prevPageIndex = prevPageIndex;
    this.prevPageOffset = prevPageOffset;
  }

  @Override
  protected final void doRedo(final OSBTreeBonsaiBucket page) {
    page.setFreeListPointer(new OBonsaiBucketPointer(pageIndex, pageOffset));
  }

  @Override
  protected final void doUndo(final OSBTreeBonsaiBucket page) {
    page.setFreeListPointer(new OBonsaiBucketPointer(prevPageIndex, prevPageOffset));
  }

  @Override
  protected void serializeToByteBuffer(final ByteBuffer buffer) {
    buffer.putInt(pageIndex);
    buffer.putInt(pageOffset);

    buffer.putInt(prevPageIndex);
    buffer.putInt(prevPageOffset);
  }

  @Override
  protected void deserializeFromByteBuffer(final ByteBuffer buffer) {
    pageIndex = buffer.getInt();
    pageOffset = buffer.getInt();

    prevPageIndex = buffer.getInt();
    prevPageOffset = buffer.getInt();
  }

  @Override
  public final byte getId() {
    return WALRecordTypes.SBTREE_BONSAI_BUCKET_SET_FREE_LIST_POINTER;
  }
}
