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

import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 14.05.13
 */
public abstract class OAbstractCheckPointStartRecord extends OAbstractWALRecord {
  private OLogSequenceNumber previousCheckpoint;

  protected OAbstractCheckPointStartRecord() {
  }

  protected OAbstractCheckPointStartRecord(final OLogSequenceNumber previousCheckpoint) {
    this.previousCheckpoint = previousCheckpoint;
  }

  public final OLogSequenceNumber getPreviousCheckpoint() {
    return previousCheckpoint;
  }

  @Override
  public int toStream(final byte[] content, int offset) {
    if (previousCheckpoint == null) {
      content[offset] = 0;
      offset++;
      return offset;
    }

    content[offset] = 1;
    offset++;

    OLongSerializer.serializeNative(previousCheckpoint.getSegment(), content, offset);
    offset += OLongSerializer.LONG_SIZE;

    OLongSerializer.serializeNative(previousCheckpoint.getPosition(), content, offset);
    offset += OLongSerializer.LONG_SIZE;

    return offset;
  }

  @Override
  public void toStream(final ByteBuffer buffer) {
    if (previousCheckpoint == null) {
      buffer.put((byte) 0);
      return;
    }

    buffer.put((byte) 1);

    buffer.putLong(previousCheckpoint.getSegment());
    buffer.putLong(previousCheckpoint.getPosition());
  }

  @Override
  public int fromStream(final byte[] content, int offset) {
    if (content[offset] == 0) {
      offset++;
      return offset;
    }

    offset++;

    final long segment = OLongSerializer.deserializeNative(content, offset);
    offset += OLongSerializer.LONG_SIZE;

    final long position = OLongSerializer.deserializeNative(content, offset);
    offset += OLongSerializer.LONG_SIZE;

    previousCheckpoint = new OLogSequenceNumber(segment, position);

    return offset;
  }

  @Override
  public int serializedSize() {
    if (previousCheckpoint == null)
      return 1;

    return 2 * OLongSerializer.LONG_SIZE + 1;
  }

  @Override
  public final boolean isUpdateMasterRecord() {
    return true;
  }

  @Override
  public final boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    final OAbstractCheckPointStartRecord that = (OAbstractCheckPointStartRecord) o;

    return Objects.equals(previousCheckpoint, that.previousCheckpoint);
  }

  @Override
  public final int hashCode() {
    return previousCheckpoint != null ? previousCheckpoint.hashCode() : 0;
  }

  @Override
  public String toString() {
    return toString("previousCheckpoint=" + previousCheckpoint);
  }
}
