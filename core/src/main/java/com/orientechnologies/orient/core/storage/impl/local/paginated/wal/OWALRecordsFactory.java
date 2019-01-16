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

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.cas.OEmptyWALRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.cas.OWriteableWALRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.bteebonsai.bonsaibucket.OBonsaiBucketAddAllPageOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.bteebonsai.bonsaibucket.OBonsaiBucketConvertToLeafPageOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.bteebonsai.bonsaibucket.OBonsaiBucketConvertToNonLeafPageOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.bteebonsai.bonsaibucket.OBonsaiBucketInsertLeafEntryPageOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.bteebonsai.bonsaibucket.OBonsaiBucketInsertNonLeafEntryPageOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.bteebonsai.bonsaibucket.OBonsaiBucketRemoveLeafEntryPageOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.bteebonsai.bonsaibucket.OBonsaiBucketRemoveNonLeafEntryPageOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.bteebonsai.bonsaibucket.OBonsaiBucketSetDeletedPageOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.bteebonsai.bonsaibucket.OBonsaiBucketSetFreeListPointerPageOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.bteebonsai.bonsaibucket.OBonsaiBucketSetLeftSiblingPageOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.bteebonsai.bonsaibucket.OBonsaiBucketSetRightSiblingPageOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.bteebonsai.bonsaibucket.OBonsaiBucketSetTreeSizePageOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.bteebonsai.bonsaibucket.OBonsaiBucketShrinkPageOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.bteebonsai.bonsaibucket.OBonsaiBucketUpdateValuePageOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.bteebonsai.bonsaisysbucket.OBonsaiSysBucketSetFreeListHeadPageOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.bteebonsai.bonsaisysbucket.OBonsaiSysBucketSetFreeListLengthPageOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.bteebonsai.bonsaisysbucket.OBonsaiSysBucketSetFreeSpacePointerPageOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.btree.btreebucket.OSBTreeBucketAddAllPageOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.btree.btreebucket.OSBTreeBucketConvertToLeafPageOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.btree.btreebucket.OSBTreeBucketConvertToNonLeafPageOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.btree.btreebucket.OSBTreeBucketInsertLeafKeyValuePageOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.btree.btreebucket.OSBTreeBucketInsertNonLeafKeyNeighboursPageOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.btree.btreebucket.OSBTreeBucketRemoveLeafEntryPageOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.btree.btreebucket.OSBTreeBucketRemoveNonLeafEntryPageOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.btree.btreebucket.OSBTreeBucketSetLeftSiblingPageOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.btree.btreebucket.OSBTreeBucketSetRightSiblingPageOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.btree.btreebucket.OSBTreeBucketSetTreeSizePageOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.btree.btreebucket.OSBTreeBucketShrinkPageOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.btree.btreebucket.OSBTreeBucketUpdateValuePageOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.btree.btreebucket.OSBtreeBucketSetFreeListFirstIndexPageOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.btree.btreenullbucket.OSBTreeNullBucketRemoveValuePageOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.btree.btreenullbucket.OSBTreeNullBucketSetValuePageOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.cluster.clusterpage.OClusterPageAppendRecordOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.cluster.clusterpage.OClusterPageDeleteRecordOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.cluster.clusterpage.OClusterPageReplaceRecordOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.cluster.clusterpage.OClusterPageSetNextPageOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.cluster.clusterpage.OClusterPageSetPrevPageOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.cluster.clusterpage.OClusterPageSetRecordLongValueOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.cluster.clusterpositionmap.OClusterPositionMapAddOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.cluster.clusterpositionmap.OClusterPositionMapAllocateOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.cluster.clusterpositionmap.OClusterPositionMapRemoveOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.cluster.clusterpositionmap.OClusterPositionMapResurrectOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.cluster.clusterpositionmap.OClusterPositionMapSetOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.cluster.clusterpositionmap.OClusterPositionMapUndoAddOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.cluster.clusterpositionmap.OClusterPositionMapUndoAllocateOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.cluster.clusterpositionmap.OClusterPositionMapUndoRemove;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.cluster.clusterpositionmap.OClusterPositionMapUndoResurrectOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.cluster.clusterpositionmap.OClusterPositionMapUndoSetOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.cluster.clusterstatevone.OClusterStateVOneSetFileSizeOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.cluster.clusterstatevone.OClusterStateVOneSetFreeListPageOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.cluster.clusterstatevone.OClusterStateVOneSetSizeOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.cluster.mapentrypoint.OMapEntryPointSetFileSizeOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.extendiblehashing.directoryfirstpage.ODirectoryFirstPageSetMaxLeftChildDepthPageOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.extendiblehashing.directoryfirstpage.ODirectoryFirstPageSetMaxRightChildDepthPageOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.extendiblehashing.directoryfirstpage.ODirectoryFirstPageSetNodeLocalDepthPageOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.extendiblehashing.directoryfirstpage.ODirectoryFirstPageSetPointerPageOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.extendiblehashing.directoryfirstpage.ODirectoryFirstPageSetTombstonePageOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.extendiblehashing.directoryfirstpage.ODirectoryFirstPageSetTreeSizePageOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.extendiblehashing.directorypage.ODirectoryPageSetMaxLeftChildDepthPageOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.extendiblehashing.directorypage.ODirectoryPageSetMaxRightChildDepthPageOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.extendiblehashing.directorypage.ODirectoryPageSetNodeLocalDepthPageOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.extendiblehashing.directorypage.ODirectoryPageSetPointerPageOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.extendiblehashing.filelevelmetadata.OFileLevelMetadataSetRecordsCountPageOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.extendiblehashing.hashindexbucket.OHashIndexBucketAddEntryPageOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.extendiblehashing.hashindexbucket.OHashIndexBucketClearAndInitPageOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.extendiblehashing.hashindexbucket.OHashIndexBucketDeleteEntryPageOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.extendiblehashing.hashindexbucket.OHashIndexBucketSetDepthPageOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.extendiblehashing.hashindexbucket.OHashIndexBucketUpdateEntryPageOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.extendiblehashing.nullbucket.OHashIndexNullBucketRemoveValuePageOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.extendiblehashing.nullbucket.OHashIndexNullBucketSetValuePageOperation;
import com.sun.jna.Native;
import com.sun.jna.Pointer;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.Map;

import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.ATOMIC_UNIT_END_RECORD;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.ATOMIC_UNIT_START_RECORD;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.CHECKPOINT_END_RECORD;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.CLUSTER_PAGE_APPEND_RECORD;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.CLUSTER_PAGE_DELETE_RECORD;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.CLUSTER_PAGE_REPLACE_RECORD;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.CLUSTER_PAGE_SET_NEXT_PAGE;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.CLUSTER_PAGE_SET_PREV_PAGE;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.CLUSTER_POSITION_MAP_ADD;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.CLUSTER_POSITION_MAP_ALLOCATE;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.CLUSTER_POSITION_MAP_REMOVE;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.CLUSTER_POSITION_MAP_RESURRECT;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.CLUSTER_POSITION_MAP_SET;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.CLUSTER_POSITION_MAP_UNDO_ADD;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.CLUSTER_POSITION_MAP_UNDO_ALLOCATE;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.CLUSTER_POSITION_MAP_UNDO_REMOVE;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.CLUSTER_POSITION_MAP_UNDO_RESURRECT;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.CLUSTER_POSITION_MAP_UNDO_SET;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.CLUSTER_SET_RECORD_LONG_VALUE;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.CLUSTER_STATE_V_ONE_SET_FILE_SIZE;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.CLUSTER_STATE_V_ONE_SET_FREE_LIST_PAGE;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.CLUSTER_STATE_V_ONE_SET_SIZE;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.DIRECTORY_FIRST_PAGE_SET_MAX_LEFT_CHILD_DEPTH;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.DIRECTORY_FIRST_PAGE_SET_MAX_RIGHT_CHILD_DEPTH;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.DIRECTORY_FIRST_PAGE_SET_NODE_LOCAL_DEPTH;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.DIRECTORY_FIRST_PAGE_SET_POINTER;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.DIRECTORY_FIRST_PAGE_SET_TOMBSTONE;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.DIRECTORY_FIRST_PAGE_SET_TREE_SIZE;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.DIRECTORY_PAGE_SET_MAX_LEFT_CHILD_DEPTH;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.DIRECTORY_PAGE_SET_MAX_RIGHT_CHILD_DEPTH;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.DIRECTORY_PAGE_SET_NODE_LOCAL_DEPTH;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.DIRECTORY_PAGE_SET_POINTER;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.EMPTY_WAL_RECORD;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.FILE_CREATED_WAL_RECORD;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.FILE_DELETED_WAL_RECORD;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.FILE_LEVEL_METADATA_SET_RECORDS_COUNT;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.FILE_TRUNCATED_WAL_RECORD;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.FULL_CHECKPOINT_START_RECORD;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.FUZZY_CHECKPOINT_END_RECORD;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.FUZZY_CHECKPOINT_START_RECORD;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.HASH_INDEX_BUCKET_ADD_ENTRY;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.HASH_INDEX_BUCKET_CLEAR_AND_INIT;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.HASH_INDEX_BUCKET_DELETE_ENTRY;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.HASH_INDEX_BUCKET_SET_DEPTH;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.HASH_INDEX_BUCKET_UPDATE_ENTRY;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.HASH_INDEX_NULL_BUCKET_REMOVE_VALUE;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.HASH_INDEX_NULL_BUCKET_SET_VALUE;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.MAP_ENTRY_POINT_SET_FILE_SIZE;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.NON_TX_OPERATION_PERFORMED_WAL_RECORD;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.SBTREE_BONSAI_BUCKET_ADD_ALL;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.SBTREE_BONSAI_BUCKET_CONVERT_TO_LEAF;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.SBTREE_BONSAI_BUCKET_CONVERT_TO_NON_LEAF;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.SBTREE_BONSAI_BUCKET_INSERT_LEAF_ENTRY;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.SBTREE_BONSAI_BUCKET_INSERT_NON_LEAF_ENTRY;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.SBTREE_BONSAI_BUCKET_REMOVE_LEAF_ENTRY;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.SBTREE_BONSAI_BUCKET_REMOVE_NON_LEAF_ENTRY;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.SBTREE_BONSAI_BUCKET_SET_DELETED;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.SBTREE_BONSAI_BUCKET_SET_FREE_LIST_POINTER;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.SBTREE_BONSAI_BUCKET_SET_LEFT_SIBLING;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.SBTREE_BONSAI_BUCKET_SET_RIGHT_SIBLING;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.SBTREE_BONSAI_BUCKET_SET_TREE_SIZE;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.SBTREE_BONSAI_BUCKET_SHRINK;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.SBTREE_BONSAI_BUCKET_UPDATE_VALUE;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.SBTREE_BONSAI_SYS_BUCKET_SET_FREE_LIST_HEAD;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.SBTREE_BONSAI_SYS_BUCKET_SET_FREE_LIST_LENGTH;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.SBTREE_BONSAI_SYS_BUCKET_SET_FREE_SPACE_POINTER;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.SBTREE_BUCKET_ADD_ALL;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.SBTREE_BUCKET_CONVERT_TO_LEAF_PAGE;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.SBTREE_BUCKET_CONVERT_TO_NON_LEAF_PAGE;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.SBTREE_BUCKET_INSERT_LEAF_KEY_VALUE;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.SBTREE_BUCKET_INSERT_NON_LEAF_KEY_NEIGHBOURS;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.SBTREE_BUCKET_REMOVE_LEAF_ENTRY;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.SBTREE_BUCKET_REMOVE_NON_LEAF_ENTRY;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.SBTREE_BUCKET_SET_FREE_LIST_FIRST_INDEX;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.SBTREE_BUCKET_SET_LEFT_SIBLING;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.SBTREE_BUCKET_SET_RIGHT_SIBLING;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.SBTREE_BUCKET_SET_TREE_SIZE;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.SBTREE_BUCKET_SHRINK;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.SBTREE_BUCKET_UPDATE_VALUE;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.SBTREE_NULL_BUCKET_REMOVE_VALUE;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.SBTREE_NULL_BUCKET_SET_VALUE;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 25.04.13
 */
public final class OWALRecordsFactory {
  private final Map<Byte, Class> idToTypeMap = new HashMap<>();

  public static final OWALRecordsFactory INSTANCE = new OWALRecordsFactory();

  private static final LZ4Factory factory                    = LZ4Factory.fastestInstance();
  private static final int        MIN_COMPRESSED_RECORD_SIZE = 2 * 1024;

  public static OPair<ByteBuffer, Long> toStream(final OWriteableWALRecord walRecord) {
    final int contentSize = walRecord.serializedSize() + 1;

    final long pointer;
    final ByteBuffer content;

    pointer = Native.malloc(contentSize);
    content = new Pointer(pointer).getByteBuffer(0, contentSize);

    final byte recordId = walRecord.getId();
    content.put(recordId);
    walRecord.toStream(content);

    if (MIN_COMPRESSED_RECORD_SIZE <= 0 || contentSize < MIN_COMPRESSED_RECORD_SIZE) {
      return new OPair<>(content, pointer);
    }

    final LZ4Compressor compressor = factory.fastCompressor();
    final int maxCompressedLength = compressor.maxCompressedLength(contentSize - 1);

    final long compressedPointer = Native.malloc(maxCompressedLength + 5);
    final ByteBuffer compressedContent = new Pointer(compressedPointer).
        getByteBuffer(0, maxCompressedLength + 5).order(ByteOrder.nativeOrder());

    content.position(1);
    compressedContent.position(5);
    final int compressedLength = compressor.compress(content, 1, contentSize - 1, compressedContent, 5, maxCompressedLength);

    if (compressedLength + 5 < contentSize) {
      if (pointer > 0) {
        Native.free(pointer);
      }

      compressedContent.limit(compressedLength + 5);
      compressedContent.put(0, (byte) (-(recordId + 1)));
      compressedContent.putInt(1, contentSize);

      return new OPair<>(compressedContent, compressedPointer);
    } else {
      Native.free(compressedPointer);
      return new OPair<>(content, pointer);
    }
  }

  public OWriteableWALRecord fromStream(byte[] content) {
    if (content[0] < 0) {
      final int originalLen = OIntegerSerializer.deserializeNative(content, 1);
      final byte[] restored = new byte[originalLen];

      final LZ4FastDecompressor decompressor = factory.fastDecompressor();
      decompressor.decompress(content, 5, restored, 1, restored.length - 1);
      restored[0] = (byte) (-content[0] - 1);
      content = restored;
    }

    final OWriteableWALRecord walRecord;
    switch (content[0]) {
    case FUZZY_CHECKPOINT_START_RECORD:
      walRecord = new OFuzzyCheckpointStartRecord();
      break;
    case FUZZY_CHECKPOINT_END_RECORD:
      walRecord = new OFuzzyCheckpointEndRecord();
      break;
    case FULL_CHECKPOINT_START_RECORD:
      walRecord = new OFullCheckpointStartRecord();
      break;
    case CHECKPOINT_END_RECORD:
      walRecord = new OCheckpointEndRecord();
      break;
    case ATOMIC_UNIT_START_RECORD:
      walRecord = new OAtomicUnitStartRecord();
      break;
    case ATOMIC_UNIT_END_RECORD:
      walRecord = new OAtomicUnitEndRecord();
      break;
    case FILE_CREATED_WAL_RECORD:
      walRecord = new OFileCreatedWALRecord();
      break;
    case NON_TX_OPERATION_PERFORMED_WAL_RECORD:
      walRecord = new ONonTxOperationPerformedWALRecord();
      break;
    case FILE_DELETED_WAL_RECORD:
      walRecord = new OFileDeletedWALRecord();
      break;
    case FILE_TRUNCATED_WAL_RECORD:
      walRecord = new OFileTruncatedWALRecord();
      break;
    case EMPTY_WAL_RECORD:
      walRecord = new OEmptyWALRecord();
      break;
    case CLUSTER_POSITION_MAP_ADD:
      walRecord = new OClusterPositionMapAddOperation();
      break;
    case CLUSTER_POSITION_MAP_ALLOCATE:
      walRecord = new OClusterPositionMapAllocateOperation();
      break;
    case CLUSTER_POSITION_MAP_SET:
      walRecord = new OClusterPositionMapSetOperation();
      break;
    case CLUSTER_POSITION_MAP_RESURRECT:
      walRecord = new OClusterPositionMapResurrectOperation();
      break;
    case CLUSTER_POSITION_MAP_REMOVE:
      walRecord = new OClusterPositionMapRemoveOperation();
      break;
    case CLUSTER_POSITION_MAP_UNDO_ADD:
      walRecord = new OClusterPositionMapUndoAddOperation();
      break;
    case CLUSTER_POSITION_MAP_UNDO_ALLOCATE:
      walRecord = new OClusterPositionMapUndoAllocateOperation();
      break;
    case CLUSTER_POSITION_MAP_UNDO_SET:
      walRecord = new OClusterPositionMapUndoSetOperation();
      break;
    case CLUSTER_POSITION_MAP_UNDO_RESURRECT:
      walRecord = new OClusterPositionMapUndoResurrectOperation();
      break;
    case CLUSTER_POSITION_MAP_UNDO_REMOVE:
      walRecord = new OClusterPositionMapUndoRemove();
      break;
    case CLUSTER_PAGE_APPEND_RECORD:
      walRecord = new OClusterPageAppendRecordOperation();
      break;
    case CLUSTER_PAGE_REPLACE_RECORD:
      walRecord = new OClusterPageReplaceRecordOperation();
      break;
    case CLUSTER_PAGE_DELETE_RECORD:
      walRecord = new OClusterPageDeleteRecordOperation();
      break;
    case CLUSTER_PAGE_SET_NEXT_PAGE:
      walRecord = new OClusterPageSetNextPageOperation();
      break;
    case CLUSTER_SET_RECORD_LONG_VALUE:
      walRecord = new OClusterPageSetRecordLongValueOperation();
      break;
    case CLUSTER_PAGE_SET_PREV_PAGE:
      walRecord = new OClusterPageSetPrevPageOperation();
      break;
    case MAP_ENTRY_POINT_SET_FILE_SIZE:
      walRecord = new OMapEntryPointSetFileSizeOperation();
      break;
    case CLUSTER_STATE_V_ONE_SET_SIZE:
      walRecord = new OClusterStateVOneSetSizeOperation();
      break;
    case CLUSTER_STATE_V_ONE_SET_FILE_SIZE:
      walRecord = new OClusterStateVOneSetFileSizeOperation();
      break;
    case CLUSTER_STATE_V_ONE_SET_FREE_LIST_PAGE:
      walRecord = new OClusterStateVOneSetFreeListPageOperation();
      break;
    case SBTREE_BUCKET_SET_TREE_SIZE:
      walRecord = new OSBTreeBucketSetTreeSizePageOperation();
      break;
    case SBTREE_BUCKET_SET_FREE_LIST_FIRST_INDEX:
      walRecord = new OSBtreeBucketSetFreeListFirstIndexPageOperation();
      break;
    case SBTREE_BUCKET_REMOVE_LEAF_ENTRY:
      walRecord = new OSBTreeBucketRemoveLeafEntryPageOperation();
      break;
    case SBTREE_BUCKET_ADD_ALL:
      walRecord = new OSBTreeBucketAddAllPageOperation();
      break;
    case SBTREE_BUCKET_SHRINK:
      walRecord = new OSBTreeBucketShrinkPageOperation();
      break;
    case SBTREE_BUCKET_INSERT_LEAF_KEY_VALUE:
      walRecord = new OSBTreeBucketInsertLeafKeyValuePageOperation();
      break;
    case SBTREE_BUCKET_INSERT_NON_LEAF_KEY_NEIGHBOURS:
      walRecord = new OSBTreeBucketInsertNonLeafKeyNeighboursPageOperation();
      break;
    case SBTREE_BUCKET_REMOVE_NON_LEAF_ENTRY:
      walRecord = new OSBTreeBucketRemoveNonLeafEntryPageOperation();
      break;
    case SBTREE_BUCKET_UPDATE_VALUE:
      walRecord = new OSBTreeBucketUpdateValuePageOperation();
      break;
    case SBTREE_BUCKET_SET_LEFT_SIBLING:
      walRecord = new OSBTreeBucketSetLeftSiblingPageOperation();
      break;
    case SBTREE_BUCKET_SET_RIGHT_SIBLING:
      walRecord = new OSBTreeBucketSetRightSiblingPageOperation();
      break;
    case SBTREE_NULL_BUCKET_SET_VALUE:
      walRecord = new OSBTreeNullBucketSetValuePageOperation();
      break;
    case SBTREE_NULL_BUCKET_REMOVE_VALUE:
      walRecord = new OSBTreeNullBucketRemoveValuePageOperation();
      break;
    case SBTREE_BUCKET_CONVERT_TO_LEAF_PAGE:
      walRecord = new OSBTreeBucketConvertToLeafPageOperation();
      break;
    case SBTREE_BUCKET_CONVERT_TO_NON_LEAF_PAGE:
      walRecord = new OSBTreeBucketConvertToNonLeafPageOperation();
      break;
    case SBTREE_BONSAI_BUCKET_SET_TREE_SIZE:
      walRecord = new OBonsaiBucketSetTreeSizePageOperation();
      break;
    case SBTREE_BONSAI_BUCKET_REMOVE_LEAF_ENTRY:
      walRecord = new OBonsaiBucketRemoveLeafEntryPageOperation();
      break;
    case SBTREE_BONSAI_BUCKET_CONVERT_TO_LEAF:
      walRecord = new OBonsaiBucketConvertToLeafPageOperation();
      break;
    case SBTREE_BONSAI_BUCKET_CONVERT_TO_NON_LEAF:
      walRecord = new OBonsaiBucketConvertToNonLeafPageOperation();
      break;
    case SBTREE_BONSAI_BUCKET_ADD_ALL:
      walRecord = new OBonsaiBucketAddAllPageOperation();
      break;
    case SBTREE_BONSAI_BUCKET_SHRINK:
      walRecord = new OBonsaiBucketShrinkPageOperation();
      break;
    case SBTREE_BONSAI_BUCKET_INSERT_LEAF_ENTRY:
      walRecord = new OBonsaiBucketInsertLeafEntryPageOperation();
      break;
    case SBTREE_BONSAI_BUCKET_INSERT_NON_LEAF_ENTRY:
      walRecord = new OBonsaiBucketInsertNonLeafEntryPageOperation();
      break;
    case SBTREE_BONSAI_BUCKET_REMOVE_NON_LEAF_ENTRY:
      walRecord = new OBonsaiBucketRemoveNonLeafEntryPageOperation();
      break;
    case SBTREE_BONSAI_BUCKET_UPDATE_VALUE:
      walRecord = new OBonsaiBucketUpdateValuePageOperation();
      break;
    case SBTREE_BONSAI_BUCKET_SET_FREE_LIST_POINTER:
      walRecord = new OBonsaiBucketSetFreeListPointerPageOperation();
      break;
    case SBTREE_BONSAI_BUCKET_SET_DELETED:
      walRecord = new OBonsaiBucketSetDeletedPageOperation();
      break;
    case SBTREE_BONSAI_BUCKET_SET_LEFT_SIBLING:
      walRecord = new OBonsaiBucketSetLeftSiblingPageOperation();
      break;
    case SBTREE_BONSAI_BUCKET_SET_RIGHT_SIBLING:
      walRecord = new OBonsaiBucketSetRightSiblingPageOperation();
      break;
    case SBTREE_BONSAI_SYS_BUCKET_SET_FREE_LIST_LENGTH:
      walRecord = new OBonsaiSysBucketSetFreeListLengthPageOperation();
      break;
    case SBTREE_BONSAI_SYS_BUCKET_SET_FREE_SPACE_POINTER:
      walRecord = new OBonsaiSysBucketSetFreeSpacePointerPageOperation();
      break;
    case SBTREE_BONSAI_SYS_BUCKET_SET_FREE_LIST_HEAD:
      walRecord = new OBonsaiSysBucketSetFreeListHeadPageOperation();
      break;
    case HASH_INDEX_BUCKET_UPDATE_ENTRY:
      walRecord = new OHashIndexBucketUpdateEntryPageOperation();
      break;
    case HASH_INDEX_BUCKET_DELETE_ENTRY:
      walRecord = new OHashIndexBucketDeleteEntryPageOperation();
      break;
    case HASH_INDEX_BUCKET_ADD_ENTRY:
      walRecord = new OHashIndexBucketAddEntryPageOperation();
      break;
    case HASH_INDEX_BUCKET_SET_DEPTH:
      walRecord = new OHashIndexBucketSetDepthPageOperation();
      break;
    case DIRECTORY_PAGE_SET_MAX_LEFT_CHILD_DEPTH:
      walRecord = new ODirectoryPageSetMaxLeftChildDepthPageOperation();
      break;
    case DIRECTORY_PAGE_SET_MAX_RIGHT_CHILD_DEPTH:
      walRecord = new ODirectoryPageSetMaxRightChildDepthPageOperation();
      break;
    case DIRECTORY_PAGE_SET_NODE_LOCAL_DEPTH:
      walRecord = new ODirectoryPageSetNodeLocalDepthPageOperation();
      break;
    case DIRECTORY_PAGE_SET_POINTER:
      walRecord = new ODirectoryPageSetPointerPageOperation();
      break;
    case FILE_LEVEL_METADATA_SET_RECORDS_COUNT:
      walRecord = new OFileLevelMetadataSetRecordsCountPageOperation();
      break;
    case HASH_INDEX_NULL_BUCKET_SET_VALUE:
      walRecord = new OHashIndexNullBucketSetValuePageOperation();
      break;
    case HASH_INDEX_NULL_BUCKET_REMOVE_VALUE:
      walRecord = new OHashIndexNullBucketRemoveValuePageOperation();
      break;
    case DIRECTORY_FIRST_PAGE_SET_TOMBSTONE:
      walRecord = new ODirectoryFirstPageSetTombstonePageOperation();
      break;
    case DIRECTORY_FIRST_PAGE_SET_TREE_SIZE:
      walRecord = new ODirectoryFirstPageSetTreeSizePageOperation();
      break;
    case DIRECTORY_FIRST_PAGE_SET_MAX_LEFT_CHILD_DEPTH:
      walRecord = new ODirectoryFirstPageSetMaxLeftChildDepthPageOperation();
      break;
    case DIRECTORY_FIRST_PAGE_SET_MAX_RIGHT_CHILD_DEPTH:
      walRecord = new ODirectoryFirstPageSetMaxRightChildDepthPageOperation();
      break;
    case DIRECTORY_FIRST_PAGE_SET_NODE_LOCAL_DEPTH:
      walRecord = new ODirectoryFirstPageSetNodeLocalDepthPageOperation();
      break;
    case DIRECTORY_FIRST_PAGE_SET_POINTER:
      walRecord = new ODirectoryFirstPageSetPointerPageOperation();
      break;
    case HASH_INDEX_BUCKET_CLEAR_AND_INIT:
      walRecord = new OHashIndexBucketClearAndInitPageOperation();
      break;
    default:
      if (idToTypeMap.containsKey(content[0])) {
        try {
          walRecord = (OWriteableWALRecord) idToTypeMap.get(content[0]).newInstance();
        } catch (final InstantiationException | IllegalAccessException e) {
          throw new IllegalStateException("Cannot deserialize passed in record", e);
        }
      } else {
        throw new IllegalStateException("Cannot deserialize passed in wal record.");
      }
    }

    walRecord.fromStream(content, 1);

    assert walRecord.getId() == content[0];
    return walRecord;
  }

  public void registerNewRecord(final byte id, final Class<? extends OWriteableWALRecord> type) {
    idToTypeMap.put(id, type);
  }
}
