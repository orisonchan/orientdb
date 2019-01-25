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
package com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations;

import com.orientechnologies.orient.core.storage.cache.OReadCache;
import com.orientechnologies.orient.core.storage.cache.OWriteCache;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitBodyRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWriteAheadLog;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.cas.OWriteableWALRecord;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Note: all atomic operations methods are designed in context that all operations on single files will be wrapped in shared lock.
 *
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 12/3/13
 */
public final class OAtomicOperation {

  private final OOperationUnitId   operationUnitId;
  private final OLogSequenceNumber startLSN;

  private int     startCounter;
  private boolean rollback;

  private final OWriteAheadLog writeAheadLog;
  private final OReadCache     readCache;
  private final OWriteCache    writeCache;

  private final Set<String> lockedObjects = new HashSet<>();

  private final Map<String, OAtomicOperationMetadata<?>> metadata = new LinkedHashMap<>();

  private       OLogSequenceNumber             lastLSN;
  private final List<OOperationUnitBodyRecord> operations = new ArrayList<>();

  private boolean keepOnlyRids;
  private int     totalLoggedSize;

  public OAtomicOperation(final OLogSequenceNumber startLSN, final OOperationUnitId operationUnitId, final OReadCache readCache,
      final OWriteCache writeCache, final OWriteAheadLog writeAheadLog) {
    this.startLSN = startLSN;
    this.lastLSN = startLSN;

    this.operationUnitId = operationUnitId;

    startCounter = 1;

    this.writeAheadLog = writeAheadLog;
    this.readCache = readCache;
    this.writeCache = writeCache;

    keepOnlyRids = writeAheadLog != null;
  }

  OLogSequenceNumber getStartLSN() {
    return startLSN;
  }

  public OLogSequenceNumber addOperation(final OOperationUnitBodyRecord operationUnitBodyRecord) throws IOException {
    operationUnitBodyRecord.setOperationUnitId(operationUnitId);

    final OLogSequenceNumber lsn;
    if (keepOnlyRids) {
      assert operations.isEmpty();

      lastLSN = writeAheadLog.log(operationUnitBodyRecord);
      lsn = lastLSN;
    } else {
      lsn = writeAheadLog.log(operationUnitBodyRecord);

      if (writeCache.isDiskBased()) {
        totalLoggedSize += operationUnitBodyRecord.getDiskSize();
        if (totalLoggedSize < 1024 * 1024) {
          operations.add(operationUnitBodyRecord);
        } else {
          operations.clear();
          keepOnlyRids = true;
        }
      } else {
        operations.add(operationUnitBodyRecord);
      }

      lastLSN = lsn;
    }

    return lsn;
  }

  void rollbackOperations() throws IOException {
    final ArrayDeque<OOperationUnitBodyRecord> records = new ArrayDeque<>(100);

    if (keepOnlyRids) {
      List<OWriteableWALRecord> readRecords = writeAheadLog.read(startLSN, 100);
      while (true) {
        for (final OWriteableWALRecord walRecord : readRecords) {
          if (walRecord instanceof OOperationUnitBodyRecord) {
            final OOperationUnitBodyRecord bodyRecord = (OOperationUnitBodyRecord) walRecord;

            if (bodyRecord.getOperationUnitId().equals(operationUnitId)) {
              records.add(bodyRecord);
            }

            if (walRecord.getLsn().compareTo(lastLSN) > 0) {
              break;
            }
          }
        }

        final OLogSequenceNumber lastReadLSN = readRecords.get(readRecords.size() - 1).getLsn();
        if (lastReadLSN.compareTo(this.lastLSN) < 0) {
          readRecords = writeAheadLog.next(lastReadLSN, 50);
        } else {
          break;
        }
      }

    } else {
      records.addAll(operations);
    }

    final Iterator<OOperationUnitBodyRecord> iterator = records.descendingIterator();
    while (iterator.hasNext()) {
      final OOperationUnitBodyRecord record = iterator.next();
      record.undo(readCache, writeCache, writeAheadLog, operationUnitId);
    }
  }

  public OOperationUnitId getOperationUnitId() {
    return operationUnitId;
  }

  /**
   * Add metadata with given key inside of atomic operation. If metadata with the same key insist inside of atomic operation it will
   * be overwritten.
   *
   * @param metadata Metadata to add.
   *
   * @see OAtomicOperationMetadata
   */
  public void addMetadata(final OAtomicOperationMetadata<?> metadata) {
    this.metadata.put(metadata.getKey(), metadata);
  }

  /**
   * @param key Key of metadata which is looking for.
   *
   * @return Metadata by associated key or <code>null</code> if such metadata is absent.
   */
  public OAtomicOperationMetadata<?> getMetadata(final String key) {
    return metadata.get(key);
  }

  /**
   * @return All keys and associated metadata contained inside of atomic operation
   */
  Map<String, OAtomicOperationMetadata<?>> getMetadata() {
    return Collections.unmodifiableMap(metadata);
  }

  void incrementCounter() {
    startCounter++;
  }

  void decrementCounter() {
    startCounter--;
  }

  public int getCounter() {
    return startCounter;
  }

  void rollbackMark() {
    rollback = true;
  }

  boolean isRollbackMarked() {
    return rollback;
  }

  void addLockedObject(final String lockedObject) {
    lockedObjects.add(lockedObject);
  }

  boolean containsInLockedObjects(final String objectToLock) {
    return lockedObjects.contains(objectToLock);
  }

  Iterable<String> lockedObjects() {
    return lockedObjects;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final OAtomicOperation operation = (OAtomicOperation) o;

    return operationUnitId.equals(operation.operationUnitId);
  }

  @Override
  public int hashCode() {
    return operationUnitId.hashCode();
  }
}
