package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.btree.btreebucket;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.index.sbtree.local.OSBTreeBucket;

import java.nio.ByteBuffer;

public final class OSBTreeBucketInsertNonLeafKeyNeighboursPageOperation extends OSBTreeBucketPageOperation {
  private int index;
  private int keySize;

  private int prevChildPointer;

  public OSBTreeBucketInsertNonLeafKeyNeighboursPageOperation() {
  }

  public OSBTreeBucketInsertNonLeafKeyNeighboursPageOperation(final int index, final int keySize, final int prevChildPointer) {
    this.index = index;
    this.keySize = keySize;

    this.prevChildPointer = prevChildPointer;
  }

  public int getIndex() {
    return index;
  }

  public int getKeySize() {
    return keySize;
  }

  public int getPrevChildPointer() {
    return prevChildPointer;
  }

  @Override
  public byte getId() {
    return WALRecordTypes.SBTREE_BUCKET_INSERT_NON_LEAF_KEY_NEIGHBOURS;
  }

  @Override
  protected void doUndo(final OSBTreeBucket page) {
    page.removeNonLeafEntry(index, keySize, prevChildPointer);
  }

  @Override
  protected void serializeToByteBuffer(final ByteBuffer buffer) {
    buffer.putInt(index);

    buffer.putInt(keySize);
    buffer.putInt(prevChildPointer);
  }

  @Override
  protected void deserializeFromByteBuffer(final ByteBuffer buffer) {
    index = buffer.getInt();
    keySize = buffer.getInt();
    prevChildPointer = buffer.getInt();
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + 3 * OIntegerSerializer.INT_SIZE;
  }
}

