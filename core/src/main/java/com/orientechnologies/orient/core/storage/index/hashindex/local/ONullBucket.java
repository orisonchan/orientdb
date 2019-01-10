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

import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.extendiblehashing.nullbucket.OHashIndexNullBucketRemoveValuePageOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.extendiblehashing.nullbucket.OHashIndexNullBucketSetValuePageOperation;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 4/25/14
 */
public final class ONullBucket<V> extends ODurablePage {

  public ONullBucket(final OCacheEntry cacheEntry) {
    super(cacheEntry);
  }

  void init() {
    setByteValue(NEXT_FREE_POSITION, (byte) 0);
  }

  public void setValue(final byte[] value, final int oldValueSize) {
    setByteValue(NEXT_FREE_POSITION, (byte) 1);

    final byte[] oldValue;
    if (oldValueSize > 0) {
      oldValue = getBinaryValue(NEXT_FREE_POSITION + 1, oldValueSize);
    } else {
      oldValue = null;
    }

    setBinaryValue(NEXT_FREE_POSITION + 1, value);
    addPageOperation(new OHashIndexNullBucketSetValuePageOperation(oldValue, value.length));
  }

  public V getValue(final OBinarySerializer<V> valueSerializer) {
    if (getByteValue(NEXT_FREE_POSITION) == 0) {
      return null;
    }

    return deserializeFromDirectMemory(valueSerializer, NEXT_FREE_POSITION + 1);
  }

  byte[] geRawValue(final OBinarySerializer<V> valueSerializer) {
    if (getByteValue(NEXT_FREE_POSITION) == 0) {
      return null;
    }

    return getBinaryValue(NEXT_FREE_POSITION + 1, getObjectSizeInDirectMemory(valueSerializer, NEXT_FREE_POSITION + 1));
  }

  public void removeValue(final int valueSize) {
    if (getByteValue(NEXT_FREE_POSITION) == 0) {
      return;
    }

    final byte[] oldValue = getBinaryValue(NEXT_FREE_POSITION + 1, valueSize);
    setByteValue(NEXT_FREE_POSITION, (byte) 0);
    addPageOperation(new OHashIndexNullBucketRemoveValuePageOperation(oldValue));
  }
}
