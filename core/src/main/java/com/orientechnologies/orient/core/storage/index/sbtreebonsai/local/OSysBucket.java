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

package com.orientechnologies.orient.core.storage.index.sbtreebonsai.local;

import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.bteebonsai.bonsaisysbucket.OBonsaiSysBucketSetFreeListHeadPageOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.bteebonsai.bonsaisysbucket.OBonsaiSysBucketSetFreeListLengthPageOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.bteebonsai.bonsaisysbucket.OBonsaiSysBucketSetFreeSpacePointerPageOperation;

/**
 * <p>
 * A system bucket for bonsai tree pages. Single per file.
 * </p>
 * <p>
 * Holds an information about:
 * </p>
 * <ul>
 * <li>head of free list</li>
 * <li>length of free list</li>
 * <li>pointer to free space</li>
 * </ul>
 *
 * @author Artem Orobets (enisher-at-gmail.com)
 */
public final class OSysBucket extends OBonsaiBucketAbstract {
  private static final int SYS_MAGIC_OFFSET        = WAL_POSITION_OFFSET + OLongSerializer.LONG_SIZE;
  private static final int FREE_SPACE_OFFSET       = SYS_MAGIC_OFFSET + OByteSerializer.BYTE_SIZE;
  private static final int FREE_LIST_HEAD_OFFSET   = FREE_SPACE_OFFSET + OBonsaiBucketPointer.SIZE;
  private static final int FREE_LIST_LENGTH_OFFSET = FREE_LIST_HEAD_OFFSET + OBonsaiBucketPointer.SIZE;

  /**
   * Magic number to check if the sys bucket is initialized.
   */
  private static final byte SYS_MAGIC = (byte) 41;

  public OSysBucket(final OCacheEntry cacheEntry) {
    super(cacheEntry);
  }

  public final void init() {
    setByteValue(SYS_MAGIC_OFFSET, SYS_MAGIC);
    setBucketPointer(FREE_SPACE_OFFSET, new OBonsaiBucketPointer(0, OSBTreeBonsaiBucket.MAX_BUCKET_SIZE_BYTES));
    setBucketPointer(FREE_LIST_HEAD_OFFSET, OBonsaiBucketPointer.NULL);
    setLongValue(FREE_LIST_LENGTH_OFFSET, 0L);
  }

  public final boolean isInitialized() {
    return getByteValue(SYS_MAGIC_OFFSET) != 41;
  }

  final long freeListLength() {
    return getLongValue(FREE_LIST_LENGTH_OFFSET);
  }

  public final void setFreeListLength(final long length) {
    final int preLength = (int) getLongValue(FREE_LIST_LENGTH_OFFSET);

    setLongValue(FREE_LIST_LENGTH_OFFSET, length);
    addPageOperation(new OBonsaiSysBucketSetFreeListLengthPageOperation(preLength));
  }

  final OBonsaiBucketPointer getFreeSpacePointer() {
    return getBucketPointer(FREE_SPACE_OFFSET);
  }

  public final void setFreeSpacePointer(final OBonsaiBucketPointer pointer) {
    final OBonsaiBucketPointer prevPointer = getBucketPointer(FREE_SPACE_OFFSET);

    setBucketPointer(FREE_SPACE_OFFSET, pointer);

    addPageOperation(
        new OBonsaiSysBucketSetFreeSpacePointerPageOperation((int) prevPointer.getPageIndex(), prevPointer.getPageOffset()));
  }

  final OBonsaiBucketPointer getFreeListHead() {
    return getBucketPointer(FREE_LIST_HEAD_OFFSET);
  }

  public final void setFreeListHead(final OBonsaiBucketPointer pointer) {
    final OBonsaiBucketPointer prevPointer = getBucketPointer(FREE_LIST_HEAD_OFFSET);

    setBucketPointer(FREE_LIST_HEAD_OFFSET, pointer);

    addPageOperation(
        new OBonsaiSysBucketSetFreeListHeadPageOperation((int) prevPointer.getPageIndex(), prevPointer.getPageOffset()));
  }
}
