package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.bteebonsai.bonsaibucket;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.serialization.serializer.binary.OBinarySerializerFactory;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.OSBTreeBonsaiBucket;

import java.nio.ByteBuffer;

public final class OBonsaiBucketAddAllPageOperation extends OBonsaiBucketPageOperation {
  private int entriesSize;

  public OBonsaiBucketAddAllPageOperation() {
  }

  public OBonsaiBucketAddAllPageOperation(final int pageOffset, final int entriesSize) {
    super(pageOffset);
    this.entriesSize = entriesSize;
  }

  int getEntriesSize() {
    return entriesSize;
  }

  @Override
  public final byte getId() {
    return WALRecordTypes.SBTREE_BONSAI_BUCKET_ADD_ALL;
  }

  @Override
  protected final void doUndo(final OSBTreeBonsaiBucket page) {
    final int size = page.size();
    final byte keySerializerId = page.getKeySerializerId();
    final byte valueSerializerId = page.getValueSerializerId();
    final OBinarySerializerFactory factory = OBinarySerializerFactory.getInstance();

    //noinspection unchecked
    page.shrink(size - entriesSize, factory.getObjectSerializer(keySerializerId), factory.getObjectSerializer(valueSerializerId));
  }

  @Override
  protected void deserializeFromByteBuffer(final ByteBuffer buffer) {
    entriesSize = buffer.getInt();
  }

  @Override
  protected void serializeToByteBuffer(final ByteBuffer buffer) {
    buffer.putInt(entriesSize);
  }

  @Override
  public final int serializedSize() {
    return super.serializedSize() + OIntegerSerializer.INT_SIZE;
  }
}
