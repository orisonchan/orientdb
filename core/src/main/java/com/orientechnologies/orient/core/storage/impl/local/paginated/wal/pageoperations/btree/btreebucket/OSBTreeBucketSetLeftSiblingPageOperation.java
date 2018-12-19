package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.btree.btreebucket;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OPageOperationRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.index.sbtree.local.OSBTreeBucket;

import java.nio.ByteBuffer;

public class OSBTreeBucketSetLeftSiblingPageOperation extends OPageOperationRecord<OSBTreeBucket> {
  private int leftSibling;
  private int prevLeftSibling;

  public OSBTreeBucketSetLeftSiblingPageOperation() {
  }

  public OSBTreeBucketSetLeftSiblingPageOperation(int leftSibling, int prevLeftSibling) {
    this.leftSibling = leftSibling;
    this.prevLeftSibling = prevLeftSibling;
  }

  @Override
  public boolean isUpdateMasterRecord() {
    return false;
  }

  @Override
  public byte getId() {
    return WALRecordTypes.SBTREE_BUCKET_SET_LEFT_SIBLING;
  }

  @Override
  protected OSBTreeBucket createPageInstance(OCacheEntry cacheEntry) {
    return new OSBTreeBucket(cacheEntry);
  }

  @Override
  protected void doRedo(OSBTreeBucket page) {
    page.setLeftSibling(leftSibling);
  }

  @Override
  protected void doUndo(OSBTreeBucket page) {
    page.setLeftSibling(prevLeftSibling);
  }

  @Override
  public int toStream(byte[] content, int offset) {
    offset = super.toStream(content, offset);

    OIntegerSerializer.INSTANCE.serializeNative(leftSibling, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    OIntegerSerializer.INSTANCE.serializeNative(prevLeftSibling, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    return offset;
  }

  @Override
  public void toStream(ByteBuffer buffer) {
    super.toStream(buffer);

    buffer.putInt(leftSibling);
    buffer.putInt(prevLeftSibling);
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    offset = super.fromStream(content, offset);

    leftSibling = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    prevLeftSibling = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    return offset;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + 2 * OIntegerSerializer.INT_SIZE;
  }
}
