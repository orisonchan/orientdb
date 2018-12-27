package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.bteebonsai.bonsaibucket;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.OSBTreeBonsaiBucket;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public final class OBonsaiBucketShrinkPageOperation extends OBonsaiBucketPageOperation {
  private List<byte[]> removedEntries;

  public OBonsaiBucketShrinkPageOperation() {
  }

  public OBonsaiBucketShrinkPageOperation(final int pageOffset, final List<byte[]> removedEntries) {
    super(pageOffset);
    this.removedEntries = removedEntries;
  }

  List<byte[]> getRemovedEntries() {
    return removedEntries;
  }

  @Override
  public final byte getId() {
    return WALRecordTypes.SBTREE_BONSAI_BUCKET_SHRINK;
  }

  @Override
  protected final void doUndo(final OSBTreeBonsaiBucket page) {
    page.addAll(removedEntries);
  }

  @Override
  protected void serializeToByteBuffer(final ByteBuffer buffer) {
    buffer.putInt(removedEntries.size());

    for (final byte[] entry : removedEntries) {
      serializeByteArray(entry, buffer);
    }
  }

  @Override
  protected void deserializeFromByteBuffer(final ByteBuffer buffer) {
    final int removedLen = buffer.getInt();
    removedEntries = new ArrayList<>(removedLen);

    for (int i = 0; i < removedLen; i++) {
      final byte[] entry = deserializeByteArray(buffer);
      removedEntries.add(entry);
    }
  }

  @Override
  public final int serializedSize() {
    int totalSize = (removedEntries.size() + 1) * OIntegerSerializer.INT_SIZE;
    for (final byte[] entry : removedEntries) {
      totalSize += entry.length;
    }

    return super.serializedSize() + totalSize;
  }
}
