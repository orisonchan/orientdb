package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.extendiblehashing.filelevelmetadata;

import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OPageOperationRecord;
import com.orientechnologies.orient.core.storage.index.hashindex.local.OHashIndexFileLevelMetadataPage;

abstract class OFileLevelMetadataPageOperation extends OPageOperationRecord<OHashIndexFileLevelMetadataPage> {

  @Override
  protected final OHashIndexFileLevelMetadataPage createPageInstance(final OCacheEntry cacheEntry) {
    return new OHashIndexFileLevelMetadataPage(cacheEntry);
  }
}
