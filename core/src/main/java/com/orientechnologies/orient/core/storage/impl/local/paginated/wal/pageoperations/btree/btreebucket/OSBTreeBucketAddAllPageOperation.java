package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.btree.btreebucket;

import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.serialization.serializer.binary.OBinarySerializerFactory;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.index.sbtree.local.OSBTreeBucket;

import java.nio.ByteBuffer;

public final class OSBTreeBucketAddAllPageOperation extends OSBTreeBucketPageOperation {
  private int     entriesCount;
  private byte    keySerializerId;
  private byte    valueSerializerId;
  private boolean isEncrypted;

  public OSBTreeBucketAddAllPageOperation() {
  }

  public OSBTreeBucketAddAllPageOperation(final byte keySerializerId, final byte valueSerializerId, final boolean isEncrypted,
      final int entriesCount) {
    this.keySerializerId = keySerializerId;
    this.valueSerializerId = valueSerializerId;
    this.isEncrypted = isEncrypted;
    this.entriesCount = entriesCount;
  }

  public int getEntriesCount() {
    return entriesCount;
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
  public byte getId() {
    return WALRecordTypes.SBTREE_BUCKET_ADD_ALL;
  }

  @Override
  protected void doUndo(final OSBTreeBucket page) {
    final OBinarySerializerFactory factory = OBinarySerializerFactory.getInstance();
    final int size = page.size();
    //noinspection unchecked
    page.shrink(size - entriesCount, factory.getObjectSerializer(keySerializerId), factory.getObjectSerializer(valueSerializerId),
        isEncrypted);
  }

  @Override
  protected void serializeToByteBuffer(final ByteBuffer buffer) {
    buffer.put(keySerializerId);
    buffer.put(valueSerializerId);

    buffer.put(isEncrypted ? (byte) 1 : 0);

    buffer.putInt(entriesCount);
  }

  @Override
  protected void deserializeFromByteBuffer(final ByteBuffer buffer) {
    keySerializerId = buffer.get();
    valueSerializerId = buffer.get();
    isEncrypted = buffer.get() > 0;
    entriesCount = buffer.getInt();
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + OIntegerSerializer.INT_SIZE + 3 * OByteSerializer.BYTE_SIZE;
  }
}
