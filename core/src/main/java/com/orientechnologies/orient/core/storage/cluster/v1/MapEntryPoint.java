package com.orientechnologies.orient.core.storage.cluster.v1;

import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.cluster.mapentrypoint.OMapEntryPointSetFileSizeOperation;

public final class MapEntryPoint extends ODurablePage {
  private static final int FILE_SIZE_OFFSET = NEXT_FREE_POSITION;

  public MapEntryPoint(final OCacheEntry cacheEntry) {
    super(cacheEntry);
  }

  int getFileSize() {
    return getIntValue(FILE_SIZE_OFFSET);
  }

  public void setFileSize(final int size) {
    final int oldSize = getIntValue(FILE_SIZE_OFFSET);

    setIntValue(FILE_SIZE_OFFSET, size);
    addPageOperation(new OMapEntryPointSetFileSizeOperation(oldSize));
  }
}
