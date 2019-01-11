package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.btree.btreebucket;

import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OPageOperationRecord;
import com.orientechnologies.orient.core.storage.index.sbtree.local.OSBTreeBucket;

public abstract class OSBTreeBucketPageOperation extends OPageOperationRecord<OSBTreeBucket> {
  @Override
  protected final OSBTreeBucket createPageInstance(final OCacheEntry cacheEntry) {
    return new OSBTreeBucket(cacheEntry);
  }
}
