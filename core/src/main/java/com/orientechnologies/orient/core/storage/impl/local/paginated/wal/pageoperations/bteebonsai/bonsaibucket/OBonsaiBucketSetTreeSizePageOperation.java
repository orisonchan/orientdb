package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.bteebonsai.bonsaibucket;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.OSBTreeBonsaiBucket;

import java.nio.ByteBuffer;

public final class OBonsaiBucketSetTreeSizePageOperation extends OBonsaiBucketPageOperation {
  private int prevTreeSize;

  public OBonsaiBucketSetTreeSizePageOperation() {
  }

  public OBonsaiBucketSetTreeSizePageOperation(final int pageOffset, final int prevTreeSize) {
    super(pageOffset);

    this.prevTreeSize = prevTreeSize;
  }

  int getPrevTreeSize() {
    return prevTreeSize;
  }

  @Override
  public byte getId() {
    return WALRecordTypes.SBTREE_BONSAI_BUCKET_SET_TREE_SIZE;
  }

  @Override
  protected void doUndo(final OSBTreeBonsaiBucket page) {
    page.setTreeSize(prevTreeSize);
  }

  @Override
  protected void deserializeFromByteBuffer(final ByteBuffer buffer) {
    prevTreeSize = buffer.getInt();
  }

  @Override
  protected void serializeToByteBuffer(final ByteBuffer buffer) {
    buffer.putInt(prevTreeSize);
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + OIntegerSerializer.INT_SIZE;
  }
}
