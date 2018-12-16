package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.cluster.clusterpositionmap;

import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cluster.OClusterPositionMapBucket;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OPageOperationRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;

public final class OClusterPositionMapAllocateOperation extends OPageOperationRecord {
  @Override
  protected void doRedo(OCacheEntry cacheEntry) {
    final OClusterPositionMapBucket bucket = new OClusterPositionMapBucket(cacheEntry, false);
    bucket.allocate();
  }

  @Override
  protected void doUndo(OCacheEntry cacheEntry) {
    final OClusterPositionMapBucket bucket = new OClusterPositionMapBucket(cacheEntry, false);
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
