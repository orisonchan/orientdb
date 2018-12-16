package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.cluster.mapentrypoint;

import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cluster.v1.MapEntryPoint;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OPageOperationRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;

public class OMapEntryPointNewPageOperation extends OPageOperationRecord {
  public OMapEntryPointNewPageOperation() {
  }

  @Override
  protected void doRedo(OCacheEntry cacheEntry) {
    new MapEntryPoint(cacheEntry, true);
  }

  @Override
  protected void doUndo(OCacheEntry cacheEntry) {
    //do nothing
  }

  @Override
  public boolean isUpdateMasterRecord() {
    return false;
  }

  @Override
  public byte getId() {
    return WALRecordTypes.MAP_ENTRY_POINT_NEW_PAGE;
  }
}
