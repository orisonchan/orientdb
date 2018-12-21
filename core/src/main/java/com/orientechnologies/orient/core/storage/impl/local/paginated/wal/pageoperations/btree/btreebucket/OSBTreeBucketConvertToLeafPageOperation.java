package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.btree.btreebucket;

import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OPageOperationRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.index.sbtree.local.OSBTreeBucket;

public final class OSBTreeBucketConvertToLeafPageOperation extends OPageOperationRecord<OSBTreeBucket> {
  @Override
  protected final OSBTreeBucket createPageInstance(final OCacheEntry cacheEntry) {
    return new OSBTreeBucket(cacheEntry);
  }

  @Override
  protected final void doRedo(final OSBTreeBucket page) {
    page.convertToLeafPage();
  }

  @Override
  protected final void doUndo(final OSBTreeBucket page) {
    page.convertToNonLeafPage();
  }

  @Override
  public final boolean isUpdateMasterRecord() {
    return false;
  }

  @Override
  public final byte getId() {
    return WALRecordTypes.SBTREE_BUCKET_CONVERT_TO_LEAF_PAGE;
  }
}
