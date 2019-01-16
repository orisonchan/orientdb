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

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 30.04.13
 */
public final class OFuzzyCheckpointStartRecord extends OAbstractCheckPointStartRecord {
  private          OLogSequenceNumber flushedLsn;

  public OFuzzyCheckpointStartRecord() {
  }

  public OFuzzyCheckpointStartRecord(final OLogSequenceNumber previousCheckpoint, final OLogSequenceNumber flushedLsn) {
    super(previousCheckpoint);

    this.flushedLsn = flushedLsn;
  }

  @Override
  public final int toStream(final byte[] content, int offset) {
    offset = super.toStream(content, offset);

    OLongSerializer.serializeNative(flushedLsn.getSegment(), content, offset);
    offset += OLongSerializer.LONG_SIZE;

    OLongSerializer.serializeNative(flushedLsn.getPosition(), content, offset);
    offset += OLongSerializer.LONG_SIZE;

    return offset;
  }

  @Override
  public final void toStream(final ByteBuffer buffer) {
    super.toStream(buffer);

    buffer.putLong(flushedLsn.getSegment());
    buffer.putLong(flushedLsn.getPosition());
  }

  @Override
  public final int fromStream(final byte[] content, int offset) {
    offset = super.fromStream(content, offset);

    final long segment = OLongSerializer.deserializeNative(content, offset);
    offset += OLongSerializer.LONG_SIZE;

    final long position = OLongSerializer.deserializeNative(content, offset);
    offset += OLongSerializer.LONG_SIZE;

    flushedLsn = new OLogSequenceNumber(segment, position);

    return offset;
  }

  @Override
  public final int serializedSize() {
    return super.serializedSize() + 2 * OLongSerializer.LONG_SIZE;
  }

  public OLogSequenceNumber getFlushedLsn() {
    return flushedLsn;
  }

  @Override
  public final byte getId() {
    return WALRecordTypes.FUZZY_CHECKPOINT_START_RECORD;
  }

  @Override
  public final String toString() {
    return "OFuzzyCheckpointStartRecord{" + "lsn=" + lsn + "} " + super.toString();
  }
}
