package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.btree.btreenullbucket;

import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OPageOperationRecord;
import com.orientechnologies.orient.core.storage.index.sbtree.local.ONullBucket;

public abstract class OSBTreeNullBucketPageOperation extends OPageOperationRecord<ONullBucket> {
  @Override
  protected final ONullBucket createPageInstance(final OCacheEntry cacheEntry) {
    return new ONullBucket(cacheEntry);
  }
}
