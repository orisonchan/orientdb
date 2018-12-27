package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.bteebonsai.bonsaibucket;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.OSBTreeBonsaiBucket;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.nio.ByteBuffer;

public final class OBonsaiBucketInsertLeafEntryPageOperation extends OBonsaiBucketPageOperation {
  private int index;
  private int keySize;
  private int valueSize;

  public OBonsaiBucketInsertLeafEntryPageOperation() {
  }

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public OBonsaiBucketInsertLeafEntryPageOperation(final int pageOffset, final int index, final int keySize, final int valueSize) {
    super(pageOffset);
    this.index = index;
    this.keySize = keySize;
    this.valueSize = valueSize;
  }

  public int getIndex() {
    return index;
  }

  public int getKeySize() {
    return keySize;
  }

  public int getValueSize() {
    return valueSize;
  }

  @Override
  public final byte getId() {
    return WALRecordTypes.SBTREE_BONSAI_BUCKET_INSERT_LEAF_ENTRY;
  }

  @Override
  protected final void doUndo(final OSBTreeBonsaiBucket page) {
    page.removeLeafEntry(index, keySize, valueSize);
  }

  @Override
  protected void serializeToByteBuffer(final ByteBuffer buffer) {
    buffer.putInt(index);
    buffer.putInt(keySize);
    buffer.putInt(valueSize);
  }

  @Override
  protected void deserializeFromByteBuffer(final ByteBuffer buffer) {
    index = buffer.getInt();
    keySize = buffer.getInt();
    valueSize = buffer.getInt();
  }

  @Override
  public final int serializedSize() {
    return super.serializedSize() + 3 * OIntegerSerializer.INT_SIZE;
  }
}
