package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.btree.btreebucket;

import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.serialization.serializer.binary.OBinarySerializerFactory;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OPageOperationRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.index.sbtree.local.OSBTreeBucket;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public final class OSBTreeBucketAddAllPageOperation extends OPageOperationRecord<OSBTreeBucket> {
  private List<byte[]> entries;
  private byte         keySerializerId;
  private byte         valueSerializerId;
  private boolean      isEncrypted;

  public OSBTreeBucketAddAllPageOperation() {
  }

  public OSBTreeBucketAddAllPageOperation(final List<byte[]> entries, final byte keySerializerId, final byte valueSerializerId,
      final boolean isEncrypted) {
    this.entries = entries;
    this.keySerializerId = keySerializerId;
    this.valueSerializerId = valueSerializerId;
    this.isEncrypted = isEncrypted;
  }

  public List<byte[]> getEntries() {
    return entries;
  }

  public byte getKeySerializerId() {
    return keySerializerId;
  }

  public byte getValueSerializerId() {
    return valueSerializerId;
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
    return WALRecordTypes.SBTREE_BUCKET_ADD_ALL;
  }

  @Override
  protected OSBTreeBucket createPageInstance(final OCacheEntry cacheEntry) {
    return new OSBTreeBucket(cacheEntry);
  }

  @Override
  protected void doRedo(final OSBTreeBucket page) {
    page.addAll(entries, keySerializerId, valueSerializerId, isEncrypted);
  }

  @Override
  protected void doUndo(final OSBTreeBucket page) {
    final OBinarySerializerFactory factory = OBinarySerializerFactory.getInstance();
    final int size = page.size();
    //noinspection unchecked
    page.shrink(size - entries.size(), factory.getObjectSerializer(keySerializerId), factory.getObjectSerializer(valueSerializerId),
        isEncrypted);
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

    OIntegerSerializer.INSTANCE.serializeNative(entries.size(), content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    for (final byte[] entry : entries) {
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

    buffer.putInt(entries.size());
    for (final byte[] entry : entries) {
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

    entries = new ArrayList<>(entriesSize);

    for (int i = 0; i < entriesSize; i++) {
      final int entrySize = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
      offset += OIntegerSerializer.INT_SIZE;

      final byte[] entry = new byte[entrySize];
      System.arraycopy(content, offset, entry, 0, entrySize);
      offset += entrySize;

      entries.add(entry);
    }

    return offset;
  }

  @Override
  public int serializedSize() {
    int totalSize = (entries.size() + 1) * OIntegerSerializer.INT_SIZE;

    for (final byte[] entry : entries) {
      totalSize += entry.length;
    }

    return super.serializedSize() + totalSize + 3 * OByteSerializer.BYTE_SIZE;
  }
}
