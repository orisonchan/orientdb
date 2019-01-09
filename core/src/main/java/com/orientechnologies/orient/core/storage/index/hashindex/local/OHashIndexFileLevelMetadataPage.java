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

package com.orientechnologies.orient.core.storage.index.hashindex.local;

import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 5/8/14
 */
final class OHashIndexFileLevelMetadataPage extends ODurablePage {

  private final static int RECORDS_COUNT_OFFSET       = NEXT_FREE_POSITION;
  private final static int KEY_SERIALIZER_ID_OFFSET   = RECORDS_COUNT_OFFSET + OLongSerializer.LONG_SIZE;
  private final static int VALUE_SERIALIZER_ID_OFFSET = KEY_SERIALIZER_ID_OFFSET + OByteSerializer.BYTE_SIZE;
  private final static int METADATA_ARRAY_OFFSET      = VALUE_SERIALIZER_ID_OFFSET + OByteSerializer.BYTE_SIZE;

  private final static int ITEM_SIZE                  = OByteSerializer.BYTE_SIZE + 3 * OLongSerializer.LONG_SIZE;

  OHashIndexFileLevelMetadataPage(final OCacheEntry cacheEntry, final boolean isNewPage) {
    super(cacheEntry);

    if (isNewPage) {
      for (int i = 0; i < OLocalHashTable.HASH_CODE_SIZE; i++)
        remove(i);

      setRecordsCount(0);
      setKeySerializerId((byte) -1);
      setValueSerializerId((byte) -1);
    }
  }

  final void setRecordsCount(final long recordsCount) {
    setLongValue(RECORDS_COUNT_OFFSET, recordsCount);
  }

  final long getRecordsCount() {
    return getLongValue(RECORDS_COUNT_OFFSET);
  }

  public final void setKeySerializerId(final byte keySerializerId) {
    setByteValue(KEY_SERIALIZER_ID_OFFSET, keySerializerId);
  }

  public final byte getKeySerializerId() {
    return getByteValue(KEY_SERIALIZER_ID_OFFSET);
  }

  public final void setValueSerializerId(final byte valueSerializerId) {
    setByteValue(VALUE_SERIALIZER_ID_OFFSET, valueSerializerId);
  }

  public final byte getValueSerializerId() {
    return getByteValue(VALUE_SERIALIZER_ID_OFFSET);
  }

  public long getFileId(final int index) {
    assert !isRemoved(index);

    int offset = METADATA_ARRAY_OFFSET + index * ITEM_SIZE;

    offset += OByteSerializer.BYTE_SIZE;
    return getLongValue(offset);
  }

  private boolean isRemoved(final int index) {
    final int offset = METADATA_ARRAY_OFFSET + index * ITEM_SIZE;
    return getByteValue(offset) == 0;
  }

  private void remove(final int index) {
    final int offset = METADATA_ARRAY_OFFSET + index * ITEM_SIZE;
    setByteValue(offset, (byte) 0);
  }
}
