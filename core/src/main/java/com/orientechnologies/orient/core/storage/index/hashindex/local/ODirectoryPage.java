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
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.extendiblehashing.directorypage.ODirectoryPageSetMaxLeftChildDepthPageOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.extendiblehashing.directorypage.ODirectoryPageSetMaxRightChildDepthPageOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.extendiblehashing.directorypage.ODirectoryPageSetNodeLocalDepthPageOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.extendiblehashing.directorypage.ODirectoryPageSetPointerPageOperation;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 5/14/14
 */
public class ODirectoryPage extends ODurablePage {
  private static final int  ITEMS_OFFSET   = NEXT_FREE_POSITION;

  static final int NODES_PER_PAGE = (OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * 1024 - ITEMS_OFFSET)
                                               / OHashTableDirectory.BINARY_LEVEL_SIZE;

  public ODirectoryPage(final OCacheEntry cacheEntry) {
    super(cacheEntry);
  }

  public final void setMaxLeftChildDepth(final int localNodeIndex, final byte maxLeftChildDepth) {
    final int offset = getItemsOffset() + localNodeIndex * OHashTableDirectory.BINARY_LEVEL_SIZE;
    final byte oldDepth = getByteValue(offset);

    setByteValue(offset, maxLeftChildDepth);
    addPageOperation(new ODirectoryPageSetMaxLeftChildDepthPageOperation(localNodeIndex, oldDepth));
  }

  final byte getMaxLeftChildDepth(final int localNodeIndex) {
    final int offset = getItemsOffset() + localNodeIndex * OHashTableDirectory.BINARY_LEVEL_SIZE;
    return getByteValue(offset);
  }

  public final void setMaxRightChildDepth(final int localNodeIndex, final byte maxRightChildDepth) {
    final int offset = getItemsOffset() + localNodeIndex * OHashTableDirectory.BINARY_LEVEL_SIZE + OByteSerializer.BYTE_SIZE;
    final byte oldDepth = getByteValue(offset);

    setByteValue(offset, maxRightChildDepth);
    addPageOperation(new ODirectoryPageSetMaxRightChildDepthPageOperation(localNodeIndex, oldDepth));
  }

  final byte getMaxRightChildDepth(final int localNodeIndex) {
    final int offset = getItemsOffset() + localNodeIndex * OHashTableDirectory.BINARY_LEVEL_SIZE + OByteSerializer.BYTE_SIZE;
    return getByteValue(offset);
  }

  public final void setNodeLocalDepth(final int localNodeIndex, final byte nodeLocalDepth) {
    final int offset = getItemsOffset() + localNodeIndex * OHashTableDirectory.BINARY_LEVEL_SIZE + 2 * OByteSerializer.BYTE_SIZE;
    final byte oldDepth = getByteValue(offset);

    setByteValue(offset, nodeLocalDepth);
    addPageOperation(new ODirectoryPageSetNodeLocalDepthPageOperation(localNodeIndex, oldDepth));
  }

  final byte getNodeLocalDepth(final int localNodeIndex) {
    final int offset = getItemsOffset() + localNodeIndex * OHashTableDirectory.BINARY_LEVEL_SIZE + 2 * OByteSerializer.BYTE_SIZE;
    return getByteValue(offset);
  }

  public final void setPointer(final int localNodeIndex, final int index, final long pointer) {
    final int offset = getItemsOffset() + (localNodeIndex * OHashTableDirectory.BINARY_LEVEL_SIZE + 3 * OByteSerializer.BYTE_SIZE)
        + index * OHashTableDirectory.ITEM_SIZE;

    final long oldPointer = getLongValue(offset);
    setLongValue(offset, pointer);

    addPageOperation(new ODirectoryPageSetPointerPageOperation(localNodeIndex, index, oldPointer));
  }

  public final long getPointer(final int localNodeIndex, final int index) {
    final int offset = getItemsOffset() + (localNodeIndex * OHashTableDirectory.BINARY_LEVEL_SIZE + 3 * OByteSerializer.BYTE_SIZE)
        + index * OHashTableDirectory.ITEM_SIZE;

    return getLongValue(offset);
  }

  int getItemsOffset() {
    return ITEMS_OFFSET;
  }
}
