package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.btree.btreebucket;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OPageOperationRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.index.sbtree.local.OSBTreeBucket;

import java.nio.ByteBuffer;

public final class OSBTreeBucketRemoveNonLeafEntryPageOperation extends OPageOperationRecord<OSBTreeBucket> {
  private int    index;
  private byte[] key;
  private int    prevChildPointer;

  private int leftNeighbour;
  private int rightNeighbour;

  public OSBTreeBucketRemoveNonLeafEntryPageOperation() {
  }

  public OSBTreeBucketRemoveNonLeafEntryPageOperation(int index, byte[] key, int prevChildPointer, int leftNeighbour,
      int rightNeighbour) {
    this.index = index;
    this.key = key;

    this.prevChildPointer = prevChildPointer;

    this.leftNeighbour = leftNeighbour;
    this.rightNeighbour = rightNeighbour;
  }

  public int getIndex() {
    return index;
  }

  public byte[] getKey() {
    return key;
  }

  public int getPrevChildPointer() {
    return prevChildPointer;
  }

  public int getLeftNeighbour() {
    return leftNeighbour;
  }

  public int getRightNeighbour() {
    return rightNeighbour;
  }

  @Override
  public boolean isUpdateMasterRecord() {
    return false;
  }

  @Override
  public byte getId() {
    return WALRecordTypes.SBTREE_BUCKET_REMOVE_NON_LEAF_ENTRY;
  }

  @Override
  protected OSBTreeBucket createPageInstance(OCacheEntry cacheEntry) {
    return new OSBTreeBucket(cacheEntry);
  }

  @Override
  protected void doRedo(OSBTreeBucket page) {
    page.removeNonLeafEntry(index, key, prevChildPointer);
  }

  @Override
  protected void doUndo(OSBTreeBucket page) {
    page.insertNonLeafKeyNeighbours(index, key, leftNeighbour, rightNeighbour,
        leftNeighbour != prevChildPointer || rightNeighbour != prevChildPointer);

  }

  @Override
  public int toStream(byte[] content, int offset) {
    offset = super.toStream(content, offset);

    OIntegerSerializer.INSTANCE.serializeNative(index, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    OIntegerSerializer.INSTANCE.serializeNative(key.length, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    System.arraycopy(key, 0, content, offset, key.length);
    offset += key.length;

    OIntegerSerializer.INSTANCE.serializeNative(leftNeighbour, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    OIntegerSerializer.INSTANCE.serializeNative(rightNeighbour, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    OIntegerSerializer.INSTANCE.serializeNative(prevChildPointer, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    return offset;
  }

  @Override
  public void toStream(ByteBuffer buffer) {
    super.toStream(buffer);

    buffer.putInt(index);
    buffer.putInt(key.length);
    buffer.put(key);
    buffer.putInt(leftNeighbour);
    buffer.putInt(rightNeighbour);
    buffer.putInt(prevChildPointer);
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    offset = super.fromStream(content, offset);

    index = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    int keyLen = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    key = new byte[keyLen];
    System.arraycopy(content, offset, key, 0, keyLen);
    offset += keyLen;

    leftNeighbour = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    rightNeighbour = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    prevChildPointer = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    return offset;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + 5 * OIntegerSerializer.INT_SIZE + key.length;
  }
}
