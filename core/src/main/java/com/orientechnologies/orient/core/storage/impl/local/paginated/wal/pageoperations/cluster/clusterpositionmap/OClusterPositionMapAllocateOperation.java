package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.cluster.clusterpositionmap;

import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cluster.OClusterPositionMapBucket;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OPageOperationRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;

public final class OClusterPositionMapAllocateOperation extends OPageOperationRecord<OClusterPositionMapBucket> {

  @Override
  protected OClusterPositionMapBucket createPageInstance(final OCacheEntry cacheEntry) {
    return new OClusterPositionMapBucket(cacheEntry, false);
  }

  @Override
  protected void doUndo(final OClusterPositionMapBucket bucket) {
    bucket.undoAllocation();
  }

  @Override
  public boolean isUpdateMasterRecord() {
    return false;
  }

  @Override
  public byte getId() {
    return WALRecordTypes.CLUSTER_POSITION_MAP_ALLOCATE;
  }
}
