package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.extendiblehashing.directorypage;

import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OPageOperationRecord;
import com.orientechnologies.orient.core.storage.index.hashindex.local.ODirectoryPage;

abstract class ODirectoryPageOperation extends OPageOperationRecord<ODirectoryPage> {
  @Override
  protected final ODirectoryPage createPageInstance(final OCacheEntry cacheEntry) {
    return new ODirectoryPage(cacheEntry);
  }
}
