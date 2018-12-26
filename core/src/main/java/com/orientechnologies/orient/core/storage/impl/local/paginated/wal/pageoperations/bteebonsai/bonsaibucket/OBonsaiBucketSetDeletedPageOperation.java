package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.bteebonsai.bonsaibucket;

import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.OSBTreeBonsaiBucket;

import java.nio.ByteBuffer;

public final class OBonsaiBucketSetDeletedPageOperation extends OBonsaiBucketPageOperation {
  private byte    keySerializerId;
  private byte    valueSerializerId;
  private boolean isLeaf;

  public OBonsaiBucketSetDeletedPageOperation() {
  }

  public OBonsaiBucketSetDeletedPageOperation(final int pageOffset, final byte keySerializerId, final byte valueSerializerId,
      final boolean isLeaf) {
    super(pageOffset);
    this.keySerializerId = keySerializerId;
    this.valueSerializerId = valueSerializerId;
    this.isLeaf = isLeaf;
  }

  @Override
  protected final void doUndo(final OSBTreeBonsaiBucket page) {
    page.init(isLeaf, keySerializerId, valueSerializerId);
  }

  @Override
  protected void serializeToByteBuffer(final ByteBuffer buffer) {
    buffer.put(keySerializerId);
    buffer.put(valueSerializerId);
    buffer.put(isLeaf ? (byte) 1 : 0);
  }

  @Override
  protected void deserializeFromByteBuffer(final ByteBuffer buffer) {
    keySerializerId = buffer.get();
    valueSerializerId = buffer.get();
    isLeaf = buffer.get() == 1;
  }

  @Override
  public final byte getId() {
    return WALRecordTypes.SBTREE_BONSAI_BUCKET_SET_DELETED;
  }

  @Override
  public final int serializedSize() {
    return super.serializedSize() + 3 * OByteSerializer.BYTE_SIZE;
  }
}
