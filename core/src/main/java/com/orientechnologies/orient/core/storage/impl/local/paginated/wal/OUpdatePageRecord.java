/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */

package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.storage.cache.OReadCache;
import com.orientechnologies.orient.core.storage.cache.OWriteCache;

import java.nio.ByteBuffer;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 26.04.13
 */
public final class OUpdatePageRecord extends OAbstractPageWALRecord {
  private OWALChanges changes;

  @SuppressWarnings("WeakerAccess")
  public OUpdatePageRecord() {
  }

  @Override
  public void undo(final OReadCache readCache, final OWriteCache writeCache, final OWriteAheadLog writeAheadLog,
      final OOperationUnitId operationUnitId) {

  }

  public OUpdatePageRecord(final long pageIndex, final long fileId, final OOperationUnitId operationUnitId,
      final OWALChanges changes) {
    super(pageIndex, fileId, operationUnitId);
    this.changes = changes;
  }

  public OWALChanges getChanges() {
    return changes;
  }

  @Override
  public final int serializedSize() {
    int serializedSize = super.serializedSize();
    serializedSize += changes.serializedSize();

    serializedSize += 2 * OLongSerializer.LONG_SIZE;

    return serializedSize;
  }

  @Override
  public final int toStream(final byte[] content, int offset) {
    offset = super.toStream(content, offset);
    offset = changes.toStream(offset, content);

    return offset;
  }

  @Override
  public final void toStream(final ByteBuffer buffer) {
    super.toStream(buffer);
    changes.toStream(buffer);
  }

  @Override
  public final int fromStream(final byte[] content, int offset) {
    offset = super.fromStream(content, offset);

    changes = new OWALPageChangesPortion();
    offset = changes.fromStream(offset, content);

    return offset;
  }

  @Override
  public final boolean isUpdateMasterRecord() {
    return false;
  }

  @Override
  public final boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    if (!super.equals(o))
      return false;

    final OUpdatePageRecord that = (OUpdatePageRecord) o;

    if (lsn == null && that.lsn == null)
      return true;

    if (lsn == null)
      return false;

    if (that.lsn == null)
      return false;

    return lsn.equals(that.lsn);
  }

  @Override
  public final int hashCode() {
    int result = super.hashCode();
    result = 31 * result + lsn.hashCode();
    return result;
  }

  @Override
  public final byte getId() {
    return WALRecordTypes.UPDATE_PAGE_RECORD;
  }
}
