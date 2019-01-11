package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.extendiblehashing.directoryfirstpage;

import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OPageOperationRecord;
import com.orientechnologies.orient.core.storage.index.hashindex.local.ODirectoryFirstPage;

public abstract class ODirectoryFirstPageOperation extends OPageOperationRecord<ODirectoryFirstPage> {
  @Override
  protected final ODirectoryFirstPage createPageInstance(final OCacheEntry cacheEntry) {
    return new ODirectoryFirstPage(cacheEntry);
  }
}
