package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.btree.btreebucket;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OPageOperationRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.index.sbtree.local.OSBTreeBucket;

import java.nio.ByteBuffer;

public class OSBTreeBucketInsertNonLeafKeyNeighboursPageOperation extends OPageOperationRecord<OSBTreeBucket> {
  private int    index;
  private byte[] serializedKey;
  private int    leftChild;
  private int    rightChild;

  private int prevChildPointer;

  public OSBTreeBucketInsertNonLeafKeyNeighboursPageOperation() {
  }

  public OSBTreeBucketInsertNonLeafKeyNeighboursPageOperation(int index, byte[] serializedKey, int leftChild, int rightChild,
      int prevChildPointer) {
    this.index = index;
    this.serializedKey = serializedKey;
    this.leftChild = leftChild;
    this.rightChild = rightChild;

    this.prevChildPointer = prevChildPointer;
  }

  @Override
  public boolean isUpdateMasterRecord() {
    return false;
  }

  @Override
  public byte getId() {
    return WALRecordTypes.SBTREE_BUCKET_INSERT_NON_LEAF_KEY_NEIGHBOURS;
  }

  @Override
  protected OSBTreeBucket createPageInstance(OCacheEntry cacheEntry) {
    return new OSBTreeBucket(cacheEntry);
  }

  @Override
  protected void doRedo(OSBTreeBucket page) {
    page.insertNonLeafKeyNeighbours(index, serializedKey, leftChild, rightChild, prevChildPointer >= 0);
  }

  @Override
  protected void doUndo(OSBTreeBucket page) {
    page.removeNonLeafEntry(index, serializedKey, prevChildPointer);
  }

  @Override
  public int toStream(byte[] content, int offset) {
    offset = super.toStream(content, offset);

    OIntegerSerializer.INSTANCE.serializeNative(index, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    OIntegerSerializer.INSTANCE.serializeNative(serializedKey.length, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    System.arraycopy(serializedKey, 0, content, offset, serializedKey.length);
    offset += serializedKey.length;

    OIntegerSerializer.INSTANCE.serializeNative(leftChild, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    OIntegerSerializer.INSTANCE.serializeNative(rightChild, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    OIntegerSerializer.INSTANCE.serializeNative(prevChildPointer, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    return offset;
  }

  @Override
  public void toStream(ByteBuffer buffer) {
    super.toStream(buffer);

    buffer.putInt(index);

    buffer.putInt(serializedKey.length);
    buffer.put(serializedKey);

    buffer.putInt(leftChild);
    buffer.putInt(rightChild);

    buffer.putInt(prevChildPointer);
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    offset = super.fromStream(content, offset);

    index = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    int keyLen = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    serializedKey = new byte[keyLen];
    System.arraycopy(content, offset, serializedKey, 0, keyLen);
    offset += keyLen;

    leftChild = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    rightChild = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    prevChildPointer = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    return offset;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + 5 * OIntegerSerializer.INT_SIZE + serializedKey.length;
  }
}

