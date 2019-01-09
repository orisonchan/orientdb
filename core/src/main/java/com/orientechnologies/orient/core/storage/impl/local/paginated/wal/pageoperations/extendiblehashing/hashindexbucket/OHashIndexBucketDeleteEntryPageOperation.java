package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.extendiblehashing.hashindexbucket;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.index.hashindex.local.OHashIndexBucket;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.nio.ByteBuffer;

@SuppressFBWarnings("EI_EXPOSE_REP2")
public final class OHashIndexBucketDeleteEntryPageOperation extends OHashIndexBucketPageOperation {
  private int    index;
  private long   hashCode;
  private byte[] key;
  private byte[] value;

  public OHashIndexBucketDeleteEntryPageOperation() {
  }

  public OHashIndexBucketDeleteEntryPageOperation(final int index, final long hashCode, final byte[] key, final byte[] value) {
    this.index = index;
    this.hashCode = hashCode;
    this.key = key;
    this.value = value;
  }

  @Override
  protected final void doUndo(final OHashIndexBucket page) {
    page.addEntry(index, hashCode, key, value);
  }

  @Override
  protected void serializeToByteBuffer(final ByteBuffer buffer) {
    buffer.putInt(index);
    buffer.putLong(hashCode);
    serializeByteArray(key, buffer);
    serializeByteArray(value, buffer);
  }

  @Override
  protected void deserializeFromByteBuffer(final ByteBuffer buffer) {
    index = buffer.getInt();
    hashCode = buffer.getLong();
    key = deserializeByteArray(buffer);
    value = deserializeByteArray(buffer);
  }

  @Override
  public final byte getId() {
    return WALRecordTypes.HASH_INDEX_BUCKET_DELETE_ENTRY;
  }

  @Override
  public final int serializedSize() {
    return super.serializedSize() + 3 * OIntegerSerializer.INT_SIZE + key.length + value.length + OLongSerializer.LONG_SIZE;
  }
}
