package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.cluster.clusterpositionmap;

import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.OReadCache;
import com.orientechnologies.orient.core.storage.cache.OWriteCache;
import com.orientechnologies.orient.core.storage.cluster.OClusterPositionMapBucket;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OPageOperationRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;

import java.io.IOException;

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
  public void undo(OReadCache readCache, OWriteCache writeCache) {
    //do nothing
  }

  @Override
  public void redo(OReadCache readCache, OWriteCache writeCache) throws IOException {
    final OCacheEntry cacheEntry = readCache.allocateNewPage(getFileId(), writeCache, true, null, false);
    try {
      new OClusterPositionMapBucket(cacheEntry, true);
    } finally {
      readCache.releaseFromWrite(cacheEntry, writeCache);
    }
  }
}
