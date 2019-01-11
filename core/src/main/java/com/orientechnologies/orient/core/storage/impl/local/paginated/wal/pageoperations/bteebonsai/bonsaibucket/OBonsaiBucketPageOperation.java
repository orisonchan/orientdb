package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.bteebonsai.bonsaibucket;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OPageOperationRecord;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.OSBTreeBonsaiBucket;

import java.nio.ByteBuffer;

abstract class OBonsaiBucketPageOperation extends OPageOperationRecord<OSBTreeBonsaiBucket> {
  private int pageOffset;

  OBonsaiBucketPageOperation() {
  }

  OBonsaiBucketPageOperation(final int pageOffset) {
    this.pageOffset = pageOffset;
  }

  public final int getPageOffset() {
    return pageOffset;
  }

  @Override
  protected final OSBTreeBonsaiBucket createPageInstance(final OCacheEntry cacheEntry) {
    return new OSBTreeBonsaiBucket(cacheEntry, pageOffset);
  }

  @Override
  protected void serializeToByteBuffer(final ByteBuffer buffer) {
    buffer.putInt(pageOffset);
  }

  @Override
  protected void deserializeFromByteBuffer(final ByteBuffer buffer) {
    pageOffset = buffer.getInt();
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + OIntegerSerializer.INT_SIZE;
  }
}
