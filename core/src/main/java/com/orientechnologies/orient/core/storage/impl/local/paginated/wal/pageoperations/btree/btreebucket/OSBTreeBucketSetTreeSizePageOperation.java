package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.btree.btreebucket;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.index.sbtree.local.OSBTreeBucket;

import java.nio.ByteBuffer;

public final class OSBTreeBucketSetTreeSizePageOperation extends OSBTreeBucketPageOperation {
  private int prevTreeSize;

  public OSBTreeBucketSetTreeSizePageOperation() {
  }

  public OSBTreeBucketSetTreeSizePageOperation(final int prevTreeSize) {
    this.prevTreeSize = prevTreeSize;
  }

  public int getPrevTreeSize() {
    return prevTreeSize;
  }

  @Override
  protected void doUndo(final OSBTreeBucket page) {
    page.setTreeSize(prevTreeSize);
  }

  @Override
  public byte getId() {
    return WALRecordTypes.SBTREE_BUCKET_SET_TREE_SIZE;
  }

  @Override
  protected void serializeToByteBuffer(final ByteBuffer buffer) {
    buffer.putInt(prevTreeSize);
  }

  @Override
  protected void deserializeFromByteBuffer(final ByteBuffer buffer) {
    prevTreeSize = buffer.getInt();
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + OIntegerSerializer.INT_SIZE;
  }
}
