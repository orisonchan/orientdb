package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.cluster.mapentrypoint;

import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cluster.v1.MapEntryPoint;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OPageOperationRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;

public class OMapEntryPointNewPageOperation extends OPageOperationRecord<MapEntryPoint> {
  public OMapEntryPointNewPageOperation() {
  }

  @Override
  protected MapEntryPoint createPageInstance(OCacheEntry cacheEntry) {
    return new MapEntryPoint(cacheEntry, true);
  }

  @Override
  protected void doRedo(MapEntryPoint entryPoint) {
    //do nothing
  }

  @Override
  protected void doUndo(MapEntryPoint entryPoint) {
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
