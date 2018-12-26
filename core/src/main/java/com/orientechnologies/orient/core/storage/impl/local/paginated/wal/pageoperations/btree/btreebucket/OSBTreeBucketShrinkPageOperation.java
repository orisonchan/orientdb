package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.btree.btreebucket;

import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OPageOperationRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.index.sbtree.local.OSBTreeBucket;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public final class OSBTreeBucketShrinkPageOperation extends OPageOperationRecord<OSBTreeBucket> {
  private List<byte[]> removedEntries;
  private byte         keySerializerId;
  private byte         valueSerializerId;
  private boolean      isEncrypted;

  public OSBTreeBucketShrinkPageOperation() {
  }

  public OSBTreeBucketShrinkPageOperation(final List<byte[]> removedEntries, final byte keySerializerId,
      final byte valueSerializerId, final boolean isEncrypted) {
    this.removedEntries = removedEntries;
    this.isEncrypted = isEncrypted;
    this.keySerializerId = keySerializerId;
    this.valueSerializerId = valueSerializerId;
  }

  public byte getKeySerializerId() {
    return keySerializerId;
  }

  public byte getValueSerializerId() {
    return valueSerializerId;
  }

  public List<byte[]> getRemovedEntries() {
    return removedEntries;
  }

  public boolean isEncrypted() {
    return isEncrypted;
  }

  @Override
  public boolean isUpdateMasterRecord() {
    return false;
  }

  @Override
  public byte getId() {
    return WALRecordTypes.SBTREE_BUCKET_SHRINK;
  }

  @Override
  protected OSBTreeBucket createPageInstance(final OCacheEntry cacheEntry) {
    return new OSBTreeBucket(cacheEntry);
  }

  @Override
  protected void doUndo(final OSBTreeBucket page) {
    page.addAll(removedEntries, keySerializerId, valueSerializerId, isEncrypted);
  }

  @Override
  public int toStream(final byte[] content, int offset) {
    offset = super.toStream(content, offset);

    content[offset] = keySerializerId;
    offset++;

    content[offset] = valueSerializerId;
    offset++;

    content[offset] = isEncrypted ? (byte) 1 : 0;
    offset++;

    OIntegerSerializer.INSTANCE.serializeNative(removedEntries.size(), content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    for (final byte[] entry : removedEntries) {
      OIntegerSerializer.INSTANCE.serializeNative(entry.length, content, offset);
      offset += OIntegerSerializer.INT_SIZE;

      System.arraycopy(entry, 0, content, offset, entry.length);
      offset += entry.length;
    }

    return offset;
  }

  @Override
  public void toStream(final ByteBuffer buffer) {
    super.toStream(buffer);

    buffer.put(keySerializerId);
    buffer.put(valueSerializerId);

    buffer.put(isEncrypted ? (byte) 1 : 0);

    buffer.putInt(removedEntries.size());

    for (final byte[] entry : removedEntries) {
      buffer.putInt(entry.length);
      buffer.put(entry);
    }
  }

  @Override
  public int fromStream(final byte[] content, int offset) {
    offset = super.fromStream(content, offset);

    keySerializerId = content[offset];
    offset++;

    valueSerializerId = content[offset];
    offset++;

    isEncrypted = content[offset] == 1;
    offset++;

    final int entriesSize = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    removedEntries = new ArrayList<>(entriesSize);

    for (int i = 0; i < entriesSize; i++) {
      final int entryLen = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
      offset += OIntegerSerializer.INT_SIZE;

      final byte[] entry = new byte[entryLen];
      System.arraycopy(content, offset, entry, 0, entryLen);
      removedEntries.add(entry);

      offset += entryLen;
    }

    return offset;
  }

  @Override
  public int serializedSize() {
    int totalSize = (removedEntries.size() + 1) * OIntegerSerializer.INT_SIZE;
    for (final byte[] entry : removedEntries) {
      totalSize += entry.length;
    }

    return super.serializedSize() + totalSize + 3 * OByteSerializer.BYTE_SIZE;
  }
}
