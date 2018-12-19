package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.btree.btreebucket;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OPageOperationRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.index.sbtree.local.OSBTreeBucket;

import java.nio.ByteBuffer;

public final class OSBTreeBucketRemoveLeafEntryPageOperation extends OPageOperationRecord<OSBTreeBucket> {
  private int    entryIndex;
  private byte[] rawKey;
  private byte[] rawValue;

  public OSBTreeBucketRemoveLeafEntryPageOperation() {
  }

  public OSBTreeBucketRemoveLeafEntryPageOperation(int entryIndex, byte[] rawKey, byte[] rawValue) {
    this.entryIndex = entryIndex;
    this.rawKey = rawKey;
    this.rawValue = rawValue;
  }

  public int getEntryIndex() {
    return entryIndex;
  }

  public byte[] getRawKey() {
    return rawKey;
  }

  public byte[] getRawValue() {
    return rawValue;
  }

  @Override
  protected OSBTreeBucket createPageInstance(OCacheEntry cacheEntry) {
    return new OSBTreeBucket(cacheEntry);
  }

  @Override
  protected void doRedo(OSBTreeBucket page) {
    page.removeLeafEntry(entryIndex, rawKey, rawValue);
  }

  @Override
  protected void doUndo(OSBTreeBucket page) {
    page.insertLeafKeyValue(entryIndex, rawKey, rawValue);
  }

  @Override
  public boolean isUpdateMasterRecord() {
    return false;
  }

  @Override
  public byte getId() {
    return WALRecordTypes.SBTREE_BUCKET_REMOVE_LEAF_ENTRY;
  }

  @Override
  public int toStream(byte[] content, int offset) {
    offset = super.toStream(content, offset);

    OIntegerSerializer.INSTANCE.serializeNative(entryIndex, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    OIntegerSerializer.INSTANCE.serializeNative(rawKey.length, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    System.arraycopy(rawKey, 0, content, offset, rawKey.length);
    offset += rawKey.length;

    OIntegerSerializer.INSTANCE.serializeNative(rawValue.length, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    System.arraycopy(rawValue, 0, content, offset, rawValue.length);
    offset += rawValue.length;

    return offset;
  }

  @Override
  public void toStream(ByteBuffer buffer) {
    super.toStream(buffer);

    buffer.putInt(entryIndex);
    buffer.putInt(rawKey.length);
    buffer.put(rawKey);
    buffer.putInt(rawValue.length);
    buffer.put(rawValue);
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    offset = super.fromStream(content, offset);

    entryIndex = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    int rawKeyLen = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    rawKey = new byte[rawKeyLen];
    System.arraycopy(content, offset, rawKey, 0, rawKeyLen);
    offset += rawKeyLen;

    int rawValueLen = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    rawValue = new byte[rawValueLen];
    System.arraycopy(content, offset, rawValue, 0, rawValueLen);
    offset += rawValueLen;

    return offset;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + 3 * OIntegerSerializer.INT_SIZE + rawKey.length + rawValue.length;
  }
}
