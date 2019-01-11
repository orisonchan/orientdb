package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.bteebonsai.bonsaibucket;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.OSBTreeBonsaiBucket;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.nio.ByteBuffer;

public final class OBonsaiBucketInsertNonLeafEntryPageOperation extends OBonsaiBucketPageOperation {
  private int index;
  private int keySize;

  private int prevChildPageIndex;
  private int prevChildPageOffset;

  public OBonsaiBucketInsertNonLeafEntryPageOperation() {
  }

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public OBonsaiBucketInsertNonLeafEntryPageOperation(final int pageOffset, final int index, final int keySize,
      final int prevChildPageIndex, final int prevChildPageOffset) {
    super(pageOffset);
    this.index = index;
    this.keySize = keySize;

    this.prevChildPageIndex = prevChildPageIndex;
    this.prevChildPageOffset = prevChildPageOffset;
  }

  public int getIndex() {
    return index;
  }

  public int getKeySize() {
    return keySize;
  }

  int getPrevChildPageIndex() {
    return prevChildPageIndex;
  }

  int getPrevChildPageOffset() {
    return prevChildPageOffset;
  }

  @Override
  public final byte getId() {
    return WALRecordTypes.SBTREE_BONSAI_BUCKET_INSERT_NON_LEAF_ENTRY;
  }

  @Override
  protected final void doUndo(final OSBTreeBonsaiBucket page) {
    page.removeNonLeafEntry(index, keySize, prevChildPageIndex, prevChildPageOffset);
  }

  @Override
  protected void serializeToByteBuffer(final ByteBuffer buffer) {
    super.serializeToByteBuffer(buffer);

    buffer.putInt(index);
    buffer.putInt(keySize);

    buffer.putInt(prevChildPageIndex);
    buffer.putInt(prevChildPageOffset);
  }

  @Override
  protected void deserializeFromByteBuffer(final ByteBuffer buffer) {
    super.deserializeFromByteBuffer(buffer);

    index = buffer.getInt();

    keySize = buffer.getInt();

    prevChildPageIndex = buffer.getInt();
    prevChildPageOffset = buffer.getInt();
  }

  @Override
  public final int serializedSize() {
    return super.serializedSize() + 4 * OIntegerSerializer.INT_SIZE;
  }
}
