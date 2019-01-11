package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.extendiblehashing.hashindexbucket;

import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OPageOperationRecord;
import com.orientechnologies.orient.core.storage.index.hashindex.local.OHashIndexBucket;

abstract class OHashIndexBucketPageOperation extends OPageOperationRecord<OHashIndexBucket> {
  @Override
  protected final OHashIndexBucket createPageInstance(final OCacheEntry cacheEntry) {
    return new OHashIndexBucket(cacheEntry);
  }
}
