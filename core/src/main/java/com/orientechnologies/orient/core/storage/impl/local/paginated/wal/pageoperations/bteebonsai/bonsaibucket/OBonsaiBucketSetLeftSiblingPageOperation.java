package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.bteebonsai.bonsaibucket;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.OBonsaiBucketPointer;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.OSBTreeBonsaiBucket;

import java.nio.ByteBuffer;

public final class OBonsaiBucketSetLeftSiblingPageOperation extends OBonsaiBucketPageOperation {
  private int leftSiblingPageIndex;
  private int leftSiblingPageOffset;

  private int prevLeftSiblingPageIndex;
  private int prevLeftSiblingPageOffset;

  public OBonsaiBucketSetLeftSiblingPageOperation() {
  }

  public OBonsaiBucketSetLeftSiblingPageOperation(final int pageOffset, final int leftSiblingPageIndex,
      final int leftSiblingPageOffset, final int prevLeftSiblingPageIndex, final int prevLeftSiblingPageOffset) {
    super(pageOffset);
    this.leftSiblingPageIndex = leftSiblingPageIndex;
    this.leftSiblingPageOffset = leftSiblingPageOffset;

    this.prevLeftSiblingPageIndex = prevLeftSiblingPageIndex;
    this.prevLeftSiblingPageOffset = prevLeftSiblingPageOffset;
  }

  @Override
  protected final void doRedo(final OSBTreeBonsaiBucket page) {
    page.setLeftSibling(new OBonsaiBucketPointer(leftSiblingPageIndex, leftSiblingPageOffset));
  }

  @Override
  protected final void doUndo(final OSBTreeBonsaiBucket page) {
    page.setLeftSibling(new OBonsaiBucketPointer(prevLeftSiblingPageIndex, prevLeftSiblingPageOffset));
  }

  @Override
  protected void serializeToByteBuffer(final ByteBuffer buffer) {
    buffer.putInt(leftSiblingPageIndex);
    buffer.putInt(leftSiblingPageOffset);

    buffer.putInt(prevLeftSiblingPageIndex);
    buffer.putInt(prevLeftSiblingPageOffset);
  }

  @Override
  protected void deserializeFromByteBuffer(final ByteBuffer buffer) {
    leftSiblingPageIndex = buffer.getInt();
    leftSiblingPageOffset = buffer.getInt();

    prevLeftSiblingPageIndex = buffer.getInt();
    prevLeftSiblingPageOffset = buffer.getInt();
  }

  @Override
  public final int serializedSize() {
    return super.serializedSize() + 4 * OIntegerSerializer.INT_SIZE;
  }

  @Override
  public final byte getId() {
    return WALRecordTypes.SBTREE_BONSAI_BUCKET_SET_LEFT_SIBLING;
  }
}
