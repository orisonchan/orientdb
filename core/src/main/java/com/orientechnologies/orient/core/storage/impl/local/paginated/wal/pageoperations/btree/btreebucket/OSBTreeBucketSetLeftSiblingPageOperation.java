package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.btree.btreebucket;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.index.sbtree.local.OSBTreeBucket;

import java.nio.ByteBuffer;

public final class OSBTreeBucketSetLeftSiblingPageOperation extends OSBTreeBucketPageOperation {
  private int prevLeftSibling;

  public OSBTreeBucketSetLeftSiblingPageOperation() {
  }

  public OSBTreeBucketSetLeftSiblingPageOperation(final int prevLeftSibling) {
    this.prevLeftSibling = prevLeftSibling;
  }

  public int getPrevLeftSibling() {
    return prevLeftSibling;
  }

  @Override
  public byte getId() {
    return WALRecordTypes.SBTREE_BUCKET_SET_LEFT_SIBLING;
  }

  @Override
  protected void doUndo(final OSBTreeBucket page) {
    page.setLeftSibling(prevLeftSibling);
  }

  @Override
  protected void serializeToByteBuffer(final ByteBuffer buffer) {
    buffer.putInt(prevLeftSibling);
  }

  @Override
  protected void deserializeFromByteBuffer(final ByteBuffer buffer) {
    prevLeftSibling = buffer.getInt();
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + OIntegerSerializer.INT_SIZE;
  }
}
