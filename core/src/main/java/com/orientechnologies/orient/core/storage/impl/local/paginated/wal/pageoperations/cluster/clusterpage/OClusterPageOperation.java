package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.cluster.clusterpage;

import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cluster.OClusterPage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OPageOperationRecord;

public abstract class OClusterPageOperation extends OPageOperationRecord<OClusterPage> {
  @Override
  protected final OClusterPage createPageInstance(final OCacheEntry cacheEntry) {
    return new OClusterPage(cacheEntry);
  }
}
