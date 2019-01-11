package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.btree.btreebucket;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.index.sbtree.local.OSBTreeBucket;

import java.nio.ByteBuffer;

public final class OSBTreeBucketConvertToNonLeafPageOperation extends OSBTreeBucketPageOperation {
  public OSBTreeBucketConvertToNonLeafPageOperation() {
  }

  @Override
  protected final void doUndo(final OSBTreeBucket page) {
    page.convertToLeafPage();
  }

  @Override
  public final byte getId() {
    return WALRecordTypes.SBTREE_BUCKET_CONVERT_TO_NON_LEAF_PAGE;
  }

  @Override
  protected void serializeToByteBuffer(final ByteBuffer buffer) {
    //do nothing
  }

  @Override
  protected void deserializeFromByteBuffer(final ByteBuffer buffer) {
    //do nothing
  }
}
