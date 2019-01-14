package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.extendiblehashing.hashindexbucket;

import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.index.hashindex.local.OHashIndexBucket;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public final class OHashIndexBucketClearAndInitPageOperation extends OHashIndexBucketPageOperation {
  private byte                            oldDepth;
  private List<OHashIndexBucket.RawEntry> entries;

  public OHashIndexBucketClearAndInitPageOperation() {
  }

  public OHashIndexBucketClearAndInitPageOperation(final byte oldDepth, final List<OHashIndexBucket.RawEntry> entries) {
    this.oldDepth = oldDepth;
    this.entries = entries;
  }

  public final byte getOldDepth() {
    return oldDepth;
  }

  public final List<OHashIndexBucket.RawEntry> getEntries() {
    return entries;
  }

  @Override
  protected final void doUndo(final OHashIndexBucket page) {
    page.init(oldDepth);
    for (final OHashIndexBucket.RawEntry rawEntry : entries) {
      page.appendEntry(rawEntry.entry, rawEntry.keySize, rawEntry.valueSize);
    }
  }

  @Override
  protected final void serializeToByteBuffer(final ByteBuffer buffer) {
    buffer.put(oldDepth);
    buffer.putInt(entries.size());

    for (final OHashIndexBucket.RawEntry rawEntry : entries) {
      serializeByteArray(rawEntry.entry, buffer);
      buffer.putInt(rawEntry.keySize);
      buffer.putInt(rawEntry.valueSize);
    }
  }

  @Override
  protected final void deserializeFromByteBuffer(final ByteBuffer buffer) {
    oldDepth = buffer.get();
    final int removedSize = buffer.getInt();
    entries = new ArrayList<>(removedSize);

    for (int i = 0; i < removedSize; i++) {
      final byte[] entry = deserializeByteArray(buffer);
      final int keySize = buffer.getInt();
      final int valueSize = buffer.getInt();
      entries.add(new OHashIndexBucket.RawEntry(entry, 0, keySize, valueSize));
    }
  }

  @Override
  public final byte getId() {
    return WALRecordTypes.HASH_INDEX_BUCKET_CLEAR_AND_INIT;
  }

  @Override
  public final int serializedSize() {
    int totalSize = OIntegerSerializer.INT_SIZE + OByteSerializer.BYTE_SIZE;
    for (final OHashIndexBucket.RawEntry rawEntry : entries) {
      totalSize += 3 * OIntegerSerializer.INT_SIZE + rawEntry.entry.length;
    }
    return super.serializedSize() + totalSize;
  }
}
