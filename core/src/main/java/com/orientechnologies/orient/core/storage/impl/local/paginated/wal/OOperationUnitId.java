/*
 * Copyright 2010-2013 OrientDB LTD (info--at--orientdb.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.common.types.OModifiableLong;
import com.orientechnologies.orient.core.OOrientListenerAbstract;
import com.orientechnologies.orient.core.Orient;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 06.06.13
 */
public final class OOperationUnitId {
  private static final AtomicLong                      sharedId        = new AtomicLong();

  private static volatile ThreadLocal<OModifiableLong> localId      = new ThreadLocal<>();
  private static volatile ThreadLocal<Long>            sharedIdCopy = new ThreadLocal<>();

  public static final int                              SERIALIZED_SIZE = 2 * OLongSerializer.LONG_SIZE;

  static {
    Orient.instance().registerListener(new OOrientListenerAbstract() {
      @Override
      public void onStartup() {
        if (localId == null)
          localId = new ThreadLocal<>();

        if (sharedIdCopy == null)
          sharedIdCopy = new ThreadLocal<>();
      }

      @Override
      public void onShutdown() {
        localId = null;
        sharedIdCopy = null;
      }
    });
  }

  private long                                         lId;
  private long                                         sId;

  public OOperationUnitId(final long lId, final long sId) {
    this.lId = lId;
    this.sId = sId;
  }

  public static OOperationUnitId generateId() {
    final OOperationUnitId operationUnitId = new OOperationUnitId();

    OModifiableLong lId = localId.get();
    if (lId == null) {
      lId = new OModifiableLong();
      localId.set(lId);
    }
    lId.increment();

    Long sId = sharedIdCopy.get();
    if (sId == null) {
      sId = sharedId.incrementAndGet();
      sharedIdCopy.set(sId);
    }

    operationUnitId.lId = lId.getValue();
    operationUnitId.sId = sId;

    return operationUnitId;
  }

  public OOperationUnitId() {
  }

  public int toStream(final byte[] content, int offset) {
    OLongSerializer.serializeNative(sId, content, offset);
    offset += OLongSerializer.LONG_SIZE;

    OLongSerializer.serializeNative(lId, content, offset);
    offset += OLongSerializer.LONG_SIZE;

    return offset;
  }

  public void toStream(final ByteBuffer buffer) {
    buffer.putLong(sId);
    buffer.putLong(lId);
  }

  public int fromStream(final byte[] content, int offset) {
    sId = OLongSerializer.deserializeNative(content, offset);
    offset += OLongSerializer.LONG_SIZE;

    lId = OLongSerializer.deserializeNative(content, offset);
    offset += OLongSerializer.LONG_SIZE;

    return offset;
  }

  @Override
  public final boolean equals(final Object o) {
    if (this == o)
      return true;
    if (!(o instanceof OOperationUnitId))
      return false;

    final OOperationUnitId that = (OOperationUnitId) o;

    if (lId != that.lId)
      return false;

    return sId == that.sId;

  }

  @Override
  public final int hashCode() {
    int result = (int) (lId ^ (lId >>> 32));
    result = 31 * result + (int) (sId ^ (sId >>> 32));
    return result;
  }

  @Override
  public final String toString() {
    return "OOperationUnitId{" + "lId=" + lId + ", sId=" + sId + '}';
  }
}
