package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.btree.btreebucket;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OPageOperationRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.index.sbtree.local.OSBTreeBucket;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.nio.ByteBuffer;

@SuppressFBWarnings({ "EI_EXPOSE_REP", "EI_EXPOSE_REP2" })
public final class OSBTreeBucketRemoveNonLeafEntryPageOperation extends OPageOperationRecord<OSBTreeBucket> {
  private int    index;
  private byte[] key;
  private int    prevChildPointer;

  private int leftNeighbour;
  private int rightNeighbour;

  public OSBTreeBucketRemoveNonLeafEntryPageOperation() {
  }

  public OSBTreeBucketRemoveNonLeafEntryPageOperation(final int index, final byte[] key, final int prevChildPointer,
      final int leftNeighbour, final int rightNeighbour) {
    this.index = index;
    this.key = key;

    this.prevChildPointer = prevChildPointer;

    this.leftNeighbour = leftNeighbour;
    this.rightNeighbour = rightNeighbour;
  }

  public int getIndex() {
    return index;
  }

  public byte[] getKey() {
    return key;
  }

  public int getPrevChildPointer() {
    return prevChildPointer;
  }

  public int getLeftNeighbour() {
    return leftNeighbour;
  }

  public int getRightNeighbour() {
    return rightNeighbour;
  }

  @Override
  public byte getId() {
    return WALRecordTypes.SBTREE_BUCKET_REMOVE_NON_LEAF_ENTRY;
  }

  @Override
  protected OSBTreeBucket createPageInstance(final OCacheEntry cacheEntry) {
    return new OSBTreeBucket(cacheEntry);
  }

  @Override
  protected void doUndo(final OSBTreeBucket page) {
    page.insertNonLeafKeyNeighbours(index, key, leftNeighbour, rightNeighbour,
        leftNeighbour != prevChildPointer || rightNeighbour != prevChildPointer);

  }

  @Override
  protected void serializeToByteBuffer(final ByteBuffer buffer) {
    buffer.putInt(index);

    serializeByteArray(key, buffer);

    buffer.putInt(leftNeighbour);
    buffer.putInt(rightNeighbour);
    buffer.putInt(prevChildPointer);
  }

  @Override
  protected void deserializeFromByteBuffer(final ByteBuffer buffer) {
    index = buffer.getInt();
    key = deserializeByteArray(buffer);

    leftNeighbour = buffer.getInt();
    rightNeighbour = buffer.getInt();
    prevChildPointer = buffer.getInt();
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + 5 * OIntegerSerializer.INT_SIZE + key.length;
  }
}
