package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.btree.btreebucket;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OPageOperationRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.index.sbtree.local.OSBTreeBucket;

import java.nio.ByteBuffer;

public final class OSBTreeBucketInsertLeafKeyValuePageOperation extends OPageOperationRecord<OSBTreeBucket> {
  private int    index;
  private byte[] serializedKey;
  private byte[] serializedValue;

  public OSBTreeBucketInsertLeafKeyValuePageOperation() {
  }

  public OSBTreeBucketInsertLeafKeyValuePageOperation(int index, byte[] serializedKey, byte[] serializedValue) {
    this.index = index;
    this.serializedKey = serializedKey;
    this.serializedValue = serializedValue;
  }

  public int getIndex() {
    return index;
  }

  public byte[] getSerializedKey() {
    return serializedKey;
  }

  public byte[] getSerializedValue() {
    return serializedValue;
  }

  @Override
  public boolean isUpdateMasterRecord() {
    return false;
  }

  @Override
  public byte getId() {
    return WALRecordTypes.SBTREE_BUCKET_INSERT_LEAF_KEY_VALUE;
  }

  @Override
  protected OSBTreeBucket createPageInstance(OCacheEntry cacheEntry) {
    return new OSBTreeBucket(cacheEntry);
  }

  @Override
  protected void doRedo(OSBTreeBucket page) {
    page.insertLeafKeyValue(index, serializedKey, serializedValue);
  }

  @Override
  protected void doUndo(OSBTreeBucket page) {
    page.removeLeafEntry(index, serializedKey, serializedValue);
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

    OIntegerSerializer.INSTANCE.serializeNative(serializedValue.length, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    System.arraycopy(serializedValue, 0, content, offset, serializedValue.length);
    offset += serializedValue.length;

    return offset;
  }

  @Override
  public void toStream(ByteBuffer buffer) {
    super.toStream(buffer);

    buffer.putInt(index);
    buffer.putInt(serializedKey.length);
    buffer.put(serializedKey);
    buffer.putInt(serializedValue.length);
    buffer.put(serializedValue);
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

    int valueLen = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    serializedValue = new byte[valueLen];
    System.arraycopy(content, offset, serializedValue, 0, valueLen);
    offset += valueLen;

    return offset;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + 3 * OIntegerSerializer.INT_SIZE + serializedValue.length + serializedKey.length;
  }
}
