package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.bteebonsai.bonsaibucket;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.OSBTreeBonsaiBucket;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.nio.ByteBuffer;

public final class OBonsaiBucketInsertLeafEntryPageOperation extends OBonsaiBucketPageOperation {
  private int    index;
  private byte[] serializedKey;
  private byte[] serializedValue;

  public OBonsaiBucketInsertLeafEntryPageOperation() {
  }

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public OBonsaiBucketInsertLeafEntryPageOperation(final int pageOffset, final int index, final byte[] serializedKey,
      final byte[] serializedValue) {
    super(pageOffset);
    this.index = index;
    this.serializedKey = serializedKey;
    this.serializedValue = serializedValue;
  }

  @Override
  public final byte getId() {
    return WALRecordTypes.SBTREE_BONSAI_BUCKET_INSERT_LEAF_ENTRY;
  }

  @Override
  protected final void doRedo(final OSBTreeBonsaiBucket page) {
    page.insertLeafEntry(index, serializedKey, serializedValue);
  }

  @Override
  protected final void doUndo(final OSBTreeBonsaiBucket page) {
    page.removeLeafEntry(index, serializedKey, serializedValue);
  }

  @Override
  protected void serializeToByteBuffer(final ByteBuffer buffer) {
    buffer.putInt(index);
    serializeByteArray(serializedKey, buffer);
    serializeByteArray(serializedValue, buffer);
  }

  @Override
  protected void deserializeFromByteBuffer(final ByteBuffer buffer) {
    index = buffer.getInt();
    serializedKey = deserializeByteArray(buffer);
    serializedValue = deserializeByteArray(buffer);
  }

  @Override
  public final int serializedSize() {
    return super.serializedSize() + 3 * OIntegerSerializer.INT_SIZE + serializedValue.length + serializedKey.length;
  }
}
