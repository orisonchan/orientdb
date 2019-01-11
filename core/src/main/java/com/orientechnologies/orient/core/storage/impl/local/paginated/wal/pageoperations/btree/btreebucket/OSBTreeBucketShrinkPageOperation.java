package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.btree.btreebucket;

import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.index.sbtree.local.OSBTreeBucket;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public final class OSBTreeBucketShrinkPageOperation extends OSBTreeBucketPageOperation {
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
  public byte getId() {
    return WALRecordTypes.SBTREE_BUCKET_SHRINK;
  }

  @Override
  protected void doUndo(final OSBTreeBucket page) {
    page.addAll(removedEntries, keySerializerId, valueSerializerId, isEncrypted);
  }

  @Override
  protected void serializeToByteBuffer(final ByteBuffer buffer) {
    buffer.put(keySerializerId);
    buffer.put(valueSerializerId);

    buffer.put(isEncrypted ? (byte) 1 : 0);

    buffer.putInt(removedEntries.size());

    for (final byte[] entry : removedEntries) {
      serializeByteArray(entry, buffer);
    }
  }

  @Override
  protected void deserializeFromByteBuffer(final ByteBuffer buffer) {
    keySerializerId = buffer.get();
    valueSerializerId = buffer.get();
    isEncrypted = buffer.get() > 0;

    final int entriesSize = buffer.getInt();
    removedEntries = new ArrayList<>(entriesSize);

    for (int i = 0; i < entriesSize; i++) {
      final byte[] entry = deserializeByteArray(buffer);
      removedEntries.add(entry);
    }
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
