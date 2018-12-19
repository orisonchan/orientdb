package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.btree.btreebucket;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OPageOperationRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.index.sbtree.local.OSBTreeBucket;

import java.nio.ByteBuffer;

public class OSBTreeBucketUpdateValuePageOperation extends OPageOperationRecord<OSBTreeBucket> {
  private int     index;
  private byte[]  value;
  private byte[]  prevValue;
  private boolean isEncrypted;

  public OSBTreeBucketUpdateValuePageOperation() {
  }

  public OSBTreeBucketUpdateValuePageOperation(int index, byte[] value, byte[] prevValue, boolean isEncrypted) {
    this.index = index;
    this.value = value;
    this.prevValue = prevValue;
    this.isEncrypted = isEncrypted;
  }

  @Override
  public boolean isUpdateMasterRecord() {
    return false;
  }

  @Override
  public byte getId() {
    return WALRecordTypes.SBTREE_BUCKET_UPDATE_VALUE;
  }

  @Override
  protected OSBTreeBucket createPageInstance(OCacheEntry cacheEntry) {
    return new OSBTreeBucket(cacheEntry);
  }

  @Override
  protected void doRedo(OSBTreeBucket page) {
    page.updateValue(index, value, prevValue, isEncrypted);
  }

  @Override
  protected void doUndo(OSBTreeBucket page) {
    page.updateValue(index, prevValue, value, isEncrypted);
  }

  @Override
  public int toStream(byte[] content, int offset) {
    offset = super.toStream(content, offset);

    OIntegerSerializer.INSTANCE.serializeNative(index, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    OIntegerSerializer.INSTANCE.serializeNative(value.length, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    System.arraycopy(value, 0, content, offset, value.length);
    offset += value.length;

    OIntegerSerializer.INSTANCE.serializeNative(prevValue.length, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    System.arraycopy(prevValue, 0, content, offset, prevValue.length);
    offset += prevValue.length;

    return offset;
  }

  @Override
  public void toStream(ByteBuffer buffer) {
    super.toStream(buffer);

    buffer.putInt(index);

    buffer.putInt(value.length);
    buffer.put(value);

    buffer.putInt(prevValue.length);
    buffer.put(prevValue);
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    offset = super.fromStream(content, offset);

    index = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    int valLen = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    value = new byte[valLen];
    System.arraycopy(content, offset, value, 0, valLen);
    offset += valLen;

    int prevValLen = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    prevValue = new byte[prevValLen];
    System.arraycopy(content, offset, prevValue, 0, prevValLen);
    offset += prevValLen;

    return offset;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + 2 * OIntegerSerializer.INT_SIZE + value.length + prevValue.length;
  }
}
