package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.bteebonsai.bonsaibucket;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.OSBTreeBonsaiBucket;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.nio.ByteBuffer;

public final class OBonsaiBucketRemoveLeafEntryPageOperation extends OBonsaiBucketPageOperation {
  private int    index;
  private byte[] prevKey;
  private byte[] prevValue;

  public OBonsaiBucketRemoveLeafEntryPageOperation() {
  }

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public OBonsaiBucketRemoveLeafEntryPageOperation(final int pageOffset, final int index, final byte[] prevKey,
      final byte[] prevValue) {
    super(pageOffset);
    this.index = index;
    this.prevKey = prevKey;
    this.prevValue = prevValue;
  }

  @Override
  public byte getId() {
    return WALRecordTypes.SBTREE_BONSAI_BUCKET_REMOVE_LEAF_ENTRY;
  }

  @Override
  protected void doRedo(final OSBTreeBonsaiBucket page) {
    page.removeLeafEntry(index, prevKey, prevValue);
  }

  @Override
  protected void doUndo(final OSBTreeBonsaiBucket page) {
    page.insertLeafEntry(index, prevKey, prevValue);
  }

  @Override
  protected void deserializeFromByteBuffer(final ByteBuffer buffer) {
    buffer.putInt(index);
    serializeByteArray(prevKey, buffer);
    serializeByteArray(prevValue, buffer);
  }

  @Override
  protected void serializeToByteBuffer(final ByteBuffer buffer) {
    index = buffer.getInt();
    prevKey = deserializeByteArray(buffer);
    prevValue = deserializeByteArray(buffer);
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + 3 * OIntegerSerializer.INT_SIZE + prevKey.length + prevValue.length;
  }
}
