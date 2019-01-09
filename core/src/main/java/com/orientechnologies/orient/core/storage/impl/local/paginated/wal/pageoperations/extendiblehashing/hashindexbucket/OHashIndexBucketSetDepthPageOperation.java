package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.extendiblehashing.hashindexbucket;

import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.index.hashindex.local.OHashIndexBucket;

import java.nio.ByteBuffer;

public final class OHashIndexBucketSetDepthPageOperation extends OHashIndexBucketPageOperation {
  private byte oldDepth;

  public OHashIndexBucketSetDepthPageOperation() {
  }

  public OHashIndexBucketSetDepthPageOperation(final byte oldDepth) {
    this.oldDepth = oldDepth;
  }

  @Override
  protected final void doUndo(final OHashIndexBucket page) {
    page.setDepth(oldDepth);
  }

  @Override
  protected void serializeToByteBuffer(final ByteBuffer buffer) {
    buffer.put(oldDepth);
  }

  @Override
  protected void deserializeFromByteBuffer(final ByteBuffer buffer) {
    oldDepth = buffer.get();
  }

  @Override
  public final byte getId() {
    return WALRecordTypes.HASH_INDEX_BUCKET_SET_DEPTH;
  }

  @Override
  public final int serializedSize() {
    return super.serializedSize() + OByteSerializer.BYTE_SIZE;
  }
}
