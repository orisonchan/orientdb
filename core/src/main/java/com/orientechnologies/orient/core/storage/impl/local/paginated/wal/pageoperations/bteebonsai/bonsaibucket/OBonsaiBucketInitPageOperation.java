package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.bteebonsai.bonsaibucket;

import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.OSBTreeBonsaiBucket;

import java.nio.ByteBuffer;

public final class OBonsaiBucketInitPageOperation extends OBonsaiBucketPageOperation {
  private boolean isLeaf;
  private byte    keySerializerId;
  private byte    valueSerializerId;

  public OBonsaiBucketInitPageOperation() {
  }

  public OBonsaiBucketInitPageOperation(final int pageOffset, final boolean isLeaf, final byte keySerializerId,
      final byte valueSerializerId) {
    super(pageOffset);

    this.isLeaf = isLeaf;
    this.keySerializerId = keySerializerId;
    this.valueSerializerId = valueSerializerId;
  }

  @Override
  public byte getId() {
    return WALRecordTypes.SBTREE_BONSAI_BUCKET_INIT;
  }

  @Override
  protected void doRedo(final OSBTreeBonsaiBucket page) {
    page.init(isLeaf, keySerializerId, valueSerializerId);
  }

  @Override
  protected void doUndo(final OSBTreeBonsaiBucket page) {
    //do nothing
  }

  @Override
  protected void deserializeFromByteBuffer(final ByteBuffer buffer) {
    keySerializerId = buffer.get();
    valueSerializerId = buffer.get();
    isLeaf = buffer.get() == 1;
  }

  @Override
  protected void serializeToByteBuffer(final ByteBuffer buffer) {
    buffer.put(keySerializerId);
    buffer.put(valueSerializerId);
    buffer.put(isLeaf ? (byte) 1 : 0);
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + 3 * OByteSerializer.BYTE_SIZE;
  }
}
