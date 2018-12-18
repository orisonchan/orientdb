package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.btree.btreebucket;

import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OPageOperationRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.index.sbtree.local.OSBTreeBucket;

import java.nio.ByteBuffer;

public final class OSBTreeBucketNewPageOperation extends OPageOperationRecord<OSBTreeBucket> {
  private boolean isLeaf;
  private byte    keySerializerId;
  private byte    valueSerializerId;

  @Override
  protected OSBTreeBucket createPageInstance(OCacheEntry cacheEntry) {
    return new OSBTreeBucket(cacheEntry, isLeaf, keySerializerId, valueSerializerId);
  }

  @Override
  protected void doRedo(OSBTreeBucket bucket) {
    //do nothing
  }

  @Override
  protected void doUndo(OSBTreeBucket bucket) {
    //do nothing
  }

  @Override
  public boolean isUpdateMasterRecord() {
    return false;
  }

  @Override
  public byte getId() {
    return WALRecordTypes.SBTREE_BUCKET_NEW_PAGE;
  }

  @Override
  public int toStream(byte[] content, int offset) {
    offset = super.toStream(content, offset);

    content[offset] = isLeaf ? (byte) 1 : 0;
    offset++;

    content[offset] = keySerializerId;
    offset++;

    content[offset] = valueSerializerId;
    offset++;

    return offset;
  }

  @Override
  public void toStream(ByteBuffer buffer) {
    super.toStream(buffer);

    buffer.put(isLeaf ? (byte) 1 : 0);
    buffer.put(keySerializerId);
    buffer.put(valueSerializerId);
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    offset = super.fromStream(content, offset);

    isLeaf = content[offset] == 1;
    offset++;

    keySerializerId = content[offset];
    offset++;

    valueSerializerId = content[offset];
    offset++;

    return offset;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + 3 * OByteSerializer.BYTE_SIZE;
  }
}
