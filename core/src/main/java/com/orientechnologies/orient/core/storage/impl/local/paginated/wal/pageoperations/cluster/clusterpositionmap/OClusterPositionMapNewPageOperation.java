package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.cluster.clusterpositionmap;

import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cluster.OClusterPositionMapBucket;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OPageOperationRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;

public final class OClusterPositionMapNewPageOperation extends OPageOperationRecord {
  public OClusterPositionMapNewPageOperation() {
  }

  @Override
  public boolean isUpdateMasterRecord() {
    return false;
  }

  @Override
  public byte getId() {
    return WALRecordTypes.CLUSTER_POSITION_MAP_NEW_PAGE;
  }

  @Override
  protected void doRedo(OCacheEntry cacheEntry) {
    new OClusterPositionMapBucket(cacheEntry, true);
  }

  @Override
  protected void doUndo(OCacheEntry cacheEntry) {
    //do nothing
  }
}
