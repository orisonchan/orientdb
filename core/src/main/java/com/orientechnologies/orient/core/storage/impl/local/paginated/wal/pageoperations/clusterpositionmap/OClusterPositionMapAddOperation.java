package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.clusterpositionmap;

import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cluster.OClusterPositionMapBucket;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OPageOperationRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;

public final class OClusterPositionMapAddOperation extends OPageOperationRecord {
  private int recordPageIndex;
  private int recordPosition;

  public OClusterPositionMapAddOperation() {
  }

  public OClusterPositionMapAddOperation(int recordPageIndex, int recordPosition) {
    super();
    this.recordPageIndex = recordPageIndex;
    this.recordPosition = recordPosition;
  }

  @Override
  public void redo(OCacheEntry cacheEntry) {
    final OClusterPositionMapBucket bucket = new OClusterPositionMapBucket(cacheEntry, false);
    bucket.add(recordPageIndex, recordPosition);
  }

  @Override
  public void undo(OCacheEntry cacheEntry) {
    final OClusterPositionMapBucket bucket = new OClusterPositionMapBucket(cacheEntry, false);
    bucket.undoAdd();
  }

  @Override
  public boolean isUpdateMasterRecord() {
    return false;
  }

  @Override
  public byte getId() {
    return WALRecordTypes.CLUSTER_POSITION_MAP_ADD;
  }
}
