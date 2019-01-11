package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.cluster.clusterstatevone;

import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cluster.v1.OPaginatedClusterStateV1;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OPageOperationRecord;

public abstract class OClusterStateVOnePageOperation extends OPageOperationRecord<OPaginatedClusterStateV1> {
  @Override
  protected final OPaginatedClusterStateV1 createPageInstance(final OCacheEntry cacheEntry) {
    return new OPaginatedClusterStateV1(cacheEntry);
  }
}
