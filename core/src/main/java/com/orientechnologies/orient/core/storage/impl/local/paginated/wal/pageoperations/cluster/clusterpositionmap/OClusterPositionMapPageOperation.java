package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.cluster.clusterpositionmap;

import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cluster.OClusterPositionMapBucket;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OPageOperationRecord;

public abstract class OClusterPositionMapPageOperation extends OPageOperationRecord<OClusterPositionMapBucket> {
  @Override
  protected final OClusterPositionMapBucket createPageInstance(final OCacheEntry cacheEntry) {
    return new OClusterPositionMapBucket(cacheEntry);
  }
}
