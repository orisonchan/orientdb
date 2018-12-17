package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.cluster.clusterpage;

import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cluster.OClusterPage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OPageOperationRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;

public final class OClusterPageNewPageOperation extends OPageOperationRecord<OClusterPage> {
  @Override
  protected OClusterPage createPageInstance(OCacheEntry cacheEntry) {
    return new OClusterPage(cacheEntry, true);
  }

  @Override
  protected void doRedo(OClusterPage clusterPage) {
    //do nothing
  }

  @Override
  protected void doUndo(OClusterPage clusterPage) {
    //do nothing
  }

  @Override
  public boolean isUpdateMasterRecord() {
    return false;
  }

  @Override
  public byte getId() {
    return WALRecordTypes.CLUSTER_PAGE_NEW;
  }
}
