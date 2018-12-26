package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import com.orientechnologies.orient.core.storage.cache.OReadCache;
import com.orientechnologies.orient.core.storage.cache.OWriteCache;

import java.io.IOException;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com) <a href="mailto:lomakin.andrey@gmail.com">Andrey Lomakin</a>
 * @since 31/12/14
 */
public abstract class OOperationUnitBodyRecord extends OOperationUnitRecord {
  protected OOperationUnitBodyRecord() {
  }

  public abstract void undo(OReadCache readCache, OWriteCache writeCache, OWriteAheadLog writeAheadLog,
      OOperationUnitId operationUnitId) throws IOException;
}