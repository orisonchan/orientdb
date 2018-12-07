package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.clusterpositionmap;

import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cluster.OClusterPositionMapBucket;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OPageOperationRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;

public final class OClusterPositionMapNewPageOperation extends OPageOperationRecord {
  public OClusterPositionMapNewPageOperation() {
  }

  public OClusterPositionMapNewPageOperation(OOperationUnitId operationUnitId, int pageIndex, long fileId) {
    super();
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
  public void undo(OCacheEntry cacheEntry) {
    //do nothing
  }

  @Override
  public void redo(OCacheEntry cacheEntry) {
    new OClusterPositionMapBucket(cacheEntry, true);
  }
}
