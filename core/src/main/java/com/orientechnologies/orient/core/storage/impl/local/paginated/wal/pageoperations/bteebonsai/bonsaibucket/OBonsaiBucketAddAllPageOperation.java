package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.bteebonsai.bonsaibucket;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.serialization.serializer.binary.OBinarySerializerFactory;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.OSBTreeBonsaiBucket;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public final class OBonsaiBucketAddAllPageOperation extends OBonsaiBucketPageOperation {
  private List<byte[]> entries;

  public OBonsaiBucketAddAllPageOperation() {
  }

  public OBonsaiBucketAddAllPageOperation(final int pageOffset, final List<byte[]> entries) {
    super(pageOffset);
    this.entries = entries;
  }

  public final List<byte[]> getEntries() {
    return entries;
  }

  @Override
  public final byte getId() {
    return WALRecordTypes.SBTREE_BONSAI_BUCKET_ADD_ALL;
  }

  @Override
  protected final void doRedo(final OSBTreeBonsaiBucket page) {
    page.addAll(entries);
  }

  @Override
  protected final void doUndo(final OSBTreeBonsaiBucket page) {
    final int size = page.size();
    final byte keySerializerId = page.getKeySerializerId();
    final byte valueSerializerId = page.getValueSerializerId();
    final OBinarySerializerFactory factory = OBinarySerializerFactory.getInstance();

    //noinspection unchecked
    page.shrink(size - entries.size(), factory.getObjectSerializer(keySerializerId),
        factory.getObjectSerializer(valueSerializerId));
  }

  @Override
  protected void deserializeFromByteBuffer(final ByteBuffer buffer) {
    final int entriesSize = buffer.getInt();
    entries = new ArrayList<>(entriesSize);

    for (int i = 0; i < entriesSize; i++) {
      final byte[] entry = deserializeByteArray(buffer);
      entries.add(entry);
    }
  }

  @Override
  protected void serializeToByteBuffer(final ByteBuffer buffer) {
    buffer.putInt(entries.size());
    for (final byte[] entry : entries) {
      serializeByteArray(entry, buffer);
    }
  }

  @Override
  public final int serializedSize() {
    int totalSize = OIntegerSerializer.INT_SIZE * (entries.size() + 1);

    for (final byte[] entry : entries) {
      totalSize += entry.length;
    }

    return super.serializedSize() + totalSize;
  }
}
