package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.bteebonsai.bonsaisysbucket;

import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OPageOperationRecord;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.OSysBucket;

abstract class OBonsaiSysBucketPageOperation extends OPageOperationRecord<OSysBucket> {
  @Override
  protected final OSysBucket createPageInstance(final OCacheEntry cacheEntry) {
    return new OSysBucket(cacheEntry);
  }
}
