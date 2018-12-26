package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.bteebonsai.bonsaibucket;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.OBonsaiBucketPointer;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.OSBTreeBonsaiBucket;

import java.nio.ByteBuffer;

public final class OBonsaiBucketSetRightSiblingPageOperation extends OBonsaiBucketPageOperation {
  private int prevRightSiblingPageIndex;
  private int prevRightSiblingOffset;

  public OBonsaiBucketSetRightSiblingPageOperation() {
  }

  public OBonsaiBucketSetRightSiblingPageOperation(final int pageOffset, final int prevRightSiblingPageIndex,
      final int prevRightSiblingOffset) {
    super(pageOffset);
    this.prevRightSiblingPageIndex = prevRightSiblingPageIndex;
    this.prevRightSiblingOffset = prevRightSiblingOffset;
  }

  @Override
  protected final void doUndo(final OSBTreeBonsaiBucket page) {
    page.setRightSibling(new OBonsaiBucketPointer(prevRightSiblingPageIndex, prevRightSiblingPageIndex));
  }

  @Override
  protected void serializeToByteBuffer(final ByteBuffer buffer) {
    buffer.putInt(prevRightSiblingPageIndex);
    buffer.putInt(prevRightSiblingOffset);

  }

  @Override
  protected void deserializeFromByteBuffer(final ByteBuffer buffer) {
    prevRightSiblingPageIndex = buffer.getInt();
    prevRightSiblingOffset = buffer.getInt();
  }

  @Override
  public final byte getId() {
    return WALRecordTypes.SBTREE_BONSAI_BUCKET_SET_RIGHT_SIBLING;
  }

  @Override
  public final int serializedSize() {
    return super.serializedSize() + 2 * OIntegerSerializer.INT_SIZE;
  }
}
