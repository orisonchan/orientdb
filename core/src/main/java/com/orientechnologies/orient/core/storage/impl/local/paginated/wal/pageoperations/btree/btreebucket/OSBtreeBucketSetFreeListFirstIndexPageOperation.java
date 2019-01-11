package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.btree.btreebucket;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.index.sbtree.local.OSBTreeBucket;

import java.nio.ByteBuffer;

public final class OSBtreeBucketSetFreeListFirstIndexPageOperation extends OSBTreeBucketPageOperation {
  private int oldPageIndex;

  public OSBtreeBucketSetFreeListFirstIndexPageOperation() {
  }

  public OSBtreeBucketSetFreeListFirstIndexPageOperation(final int oldPageIndex) {
    this.oldPageIndex = oldPageIndex;
  }

  public int getPrevPageIndex() {
    return oldPageIndex;
  }

  @Override
  protected void doUndo(final OSBTreeBucket page) {
    page.setValuesFreeListFirstIndex(oldPageIndex);
  }

  @Override
  public byte getId() {
    return WALRecordTypes.SBTREE_BUCKET_SET_FREE_LIST_FIRST_INDEX;
  }

  @Override
  protected void serializeToByteBuffer(final ByteBuffer buffer) {
    buffer.putInt(oldPageIndex);
  }

  @Override
  protected void deserializeFromByteBuffer(final ByteBuffer buffer) {
    oldPageIndex = buffer.getInt();
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + OIntegerSerializer.INT_SIZE;
  }
}
