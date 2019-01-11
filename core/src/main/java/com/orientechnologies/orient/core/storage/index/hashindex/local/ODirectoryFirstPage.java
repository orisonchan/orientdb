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

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.extendiblehashing.directoryfirstpage.ODirectoryFirstPageSetMaxLeftChildDepthPageOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.extendiblehashing.directoryfirstpage.ODirectoryFirstPageSetMaxRightChildDepthPageOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.extendiblehashing.directoryfirstpage.ODirectoryFirstPageSetNodeLocalDepthPageOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.extendiblehashing.directoryfirstpage.ODirectoryFirstPageSetPointerPageOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.extendiblehashing.directoryfirstpage.ODirectoryFirstPageSetTombstonePageOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.extendiblehashing.directoryfirstpage.ODirectoryFirstPageSetTreeSizePageOperation;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 5/14/14
 */
public final class ODirectoryFirstPage extends ODirectoryPage {
  private static final int TREE_SIZE_OFFSET = NEXT_FREE_POSITION;
  private static final int TOMBSTONE_OFFSET = TREE_SIZE_OFFSET + OIntegerSerializer.INT_SIZE;

  private static final int ITEMS_OFFSET = TOMBSTONE_OFFSET + OIntegerSerializer.INT_SIZE;

  static final int NODES_PER_PAGE =
      (OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * 1024 - ITEMS_OFFSET) / OHashTableDirectory.BINARY_LEVEL_SIZE;

  public ODirectoryFirstPage(final OCacheEntry cacheEntry) {
    super(cacheEntry);
  }

  public final void setTreeSize(final int treeSize) {
    final int oldTreeSize = getIntValue(TREE_SIZE_OFFSET);

    setIntValue(TREE_SIZE_OFFSET, treeSize);
    addPageOperation(new ODirectoryFirstPageSetTreeSizePageOperation(oldTreeSize));
  }

  final int getTreeSize() {
    return getIntValue(TREE_SIZE_OFFSET);
  }

  public final void setTombstone(final int tombstone) {
    final int oldTombstone = getIntValue(TOMBSTONE_OFFSET);

    setIntValue(TOMBSTONE_OFFSET, tombstone);
    addPageOperation(new ODirectoryFirstPageSetTombstonePageOperation(oldTombstone));
  }

  final int getTombstone() {
    return getIntValue(TOMBSTONE_OFFSET);
  }

  @Override
  protected int getItemsOffset() {
    return ITEMS_OFFSET;
  }

  @Override
  void logSetMaxLeftChildDepth(final int localNodeIndex, final byte oldDepth) {
    addPageOperation(new ODirectoryFirstPageSetMaxLeftChildDepthPageOperation(localNodeIndex, oldDepth));
  }

  @Override
  void logSetMaxRightChildDepth(final int localNodeIndex, final byte oldDepth) {
    addPageOperation(new ODirectoryFirstPageSetMaxRightChildDepthPageOperation(localNodeIndex, oldDepth));
  }

  @Override
  void logSetNodeLocalDepth(final int localNodeIndex, final byte oldDepth) {
    addPageOperation(new ODirectoryFirstPageSetNodeLocalDepthPageOperation(localNodeIndex, oldDepth));
  }

  @Override
  void logSetPointer(final int localNodeIndex, final int index, final long oldPointer) {
    addPageOperation(new ODirectoryFirstPageSetPointerPageOperation(localNodeIndex, index, oldPointer));
  }
}
