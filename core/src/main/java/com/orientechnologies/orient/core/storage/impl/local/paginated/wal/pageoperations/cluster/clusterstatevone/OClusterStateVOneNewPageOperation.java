package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.cluster.clusterstatevone;

import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cluster.v1.OPaginatedClusterStateV1;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OPageOperationRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;

public class OClusterStateVOneNewPageOperation extends OPageOperationRecord<OPaginatedClusterStateV1> {
  public OClusterStateVOneNewPageOperation() {
  }

  @Override
  protected OPaginatedClusterStateV1 createPageInstance(OCacheEntry cacheEntry) {
    return new OPaginatedClusterStateV1(cacheEntry, true);
  }

  @Override
  protected void doRedo(OPaginatedClusterStateV1 clusterState) {
    //do nothing
  }

  @Override
  protected void doUndo(OPaginatedClusterStateV1 clusterState) {
    //do nothing
  }

  @Override
  public boolean isUpdateMasterRecord() {
    return false;
  }

  @Override
  public byte getId() {
    return WALRecordTypes.CLUSTER_STATE_V_ONE_NEW_PAGE;
  }
}
