package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.bteebonsai.bonsaibucket;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OPageOperationRecord;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.OSBTreeBonsaiBucket;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

abstract class OBonsaiBucketPageOperation extends OPageOperationRecord<OSBTreeBonsaiBucket> {
  private int pageOffset;

  OBonsaiBucketPageOperation() {
  }

  OBonsaiBucketPageOperation(final int pageOffset) {
    this.pageOffset = pageOffset;
  }

  @Override
  public final boolean isUpdateMasterRecord() {
    return false;
  }

  @Override
  protected final OSBTreeBonsaiBucket createPageInstance(final OCacheEntry cacheEntry) {
    return new OSBTreeBonsaiBucket(cacheEntry, pageOffset);
  }

  @Override
  public final int toStream(final byte[] content, int offset) {
    offset = super.toStream(content, offset);

    final ByteBuffer buffer = createNativeByteBuffer(content, offset);
    buffer.putInt(pageOffset);
    serializeToByteBuffer(buffer);

    return buffer.position();
  }

  @Override
  public final void toStream(final ByteBuffer buffer) {
    super.toStream(buffer);

    buffer.putInt(pageOffset);
    serializeToByteBuffer(buffer);
  }

  @Override
  public final int fromStream(final byte[] content, int offset) {
    offset = super.fromStream(content, offset);

    final ByteBuffer buffer = createNativeByteBuffer(content, offset);

    pageOffset = buffer.getInt();

    deserializeFromByteBuffer(buffer);

    return buffer.position();
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + OIntegerSerializer.INT_SIZE;
  }

  private static ByteBuffer createNativeByteBuffer(final byte[] content, final int offset) {
    return ByteBuffer.wrap(content, offset, content.length).order(ByteOrder.nativeOrder());
  }

  static void serializeByteArray(final byte[] value, final ByteBuffer buffer) {
    buffer.putInt(value.length);
    buffer.put(value);
  }

  static byte[] deserializeByteArray(final ByteBuffer buffer) {
    final int len = buffer.getInt();
    final byte[] value = new byte[len];
    buffer.get(value);

    return value;
  }

  protected abstract void serializeToByteBuffer(ByteBuffer buffer);

  protected abstract void deserializeFromByteBuffer(ByteBuffer buffer);
}
