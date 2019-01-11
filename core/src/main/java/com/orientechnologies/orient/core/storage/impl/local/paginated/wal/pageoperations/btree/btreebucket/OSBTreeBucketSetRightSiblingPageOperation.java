package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.btree.btreebucket;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.index.sbtree.local.OSBTreeBucket;

import java.nio.ByteBuffer;

public final class OSBTreeBucketSetRightSiblingPageOperation extends OSBTreeBucketPageOperation {
  private int prevRightSibling;

  public OSBTreeBucketSetRightSiblingPageOperation() {
  }

  public OSBTreeBucketSetRightSiblingPageOperation(final int rightSibling, final int prevRightSibling) {
    this.prevRightSibling = prevRightSibling;
  }

  public int getPrevRightSibling() {
    return prevRightSibling;
  }

  @Override
  public byte getId() {
    return WALRecordTypes.SBTREE_BUCKET_SET_RIGHT_SIBLING;
  }

  @Override
  protected void doUndo(final OSBTreeBucket page) {
    page.setRightSibling(prevRightSibling);
  }

  @Override
  protected void serializeToByteBuffer(final ByteBuffer buffer) {
    buffer.putInt(prevRightSibling);
  }

  @Override
  protected void deserializeFromByteBuffer(final ByteBuffer buffer) {
    prevRightSibling = buffer.getInt();
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + OIntegerSerializer.INT_SIZE;
  }
}
