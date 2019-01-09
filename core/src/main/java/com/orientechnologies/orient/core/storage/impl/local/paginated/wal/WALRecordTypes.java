package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

@SuppressWarnings("WeakerAccess")
public final class WALRecordTypes {
  public static final byte UPDATE_PAGE_RECORD                    = 0;
  public static final byte FUZZY_CHECKPOINT_START_RECORD         = 1;
  public static final byte FUZZY_CHECKPOINT_END_RECORD           = 2;
  public static final byte FULL_CHECKPOINT_START_RECORD          = 4;
  public static final byte CHECKPOINT_END_RECORD                 = 5;
  public static final byte ATOMIC_UNIT_START_RECORD              = 8;
  public static final byte ATOMIC_UNIT_END_RECORD                = 9;
  public static final byte FILE_CREATED_WAL_RECORD               = 10;
  public static final byte NON_TX_OPERATION_PERFORMED_WAL_RECORD = 11;
  public static final byte FILE_DELETED_WAL_RECORD               = 12;
  public static final byte FILE_TRUNCATED_WAL_RECORD             = 13;
  public static final byte EMPTY_WAL_RECORD                      = 14;

  public static final byte CLUSTER_POSITION_MAP_ADD            = 16;
  public static final byte CLUSTER_POSITION_MAP_ALLOCATE       = 17;
  public static final byte CLUSTER_POSITION_MAP_SET            = 18;
  public static final byte CLUSTER_POSITION_MAP_RESURRECT      = 19;
  public static final byte CLUSTER_POSITION_MAP_REMOVE         = 20;
  public static final byte CLUSTER_POSITION_MAP_UNDO_ADD       = 21;
  public static final byte CLUSTER_POSITION_MAP_UNDO_ALLOCATE  = 22;
  public static final byte CLUSTER_POSITION_MAP_UNDO_SET       = 23;
  public static final byte CLUSTER_POSITION_MAP_UNDO_RESURRECT = 24;
  public static final byte CLUSTER_POSITION_MAP_UNDO_REMOVE    = 25;

  public static final byte CLUSTER_PAGE_APPEND_RECORD    = 27;
  public static final byte CLUSTER_PAGE_REPLACE_RECORD   = 28;
  public static final byte CLUSTER_PAGE_DELETE_RECORD    = 29;
  public static final byte CLUSTER_PAGE_SET_NEXT_PAGE    = 30;
  public static final byte CLUSTER_SET_RECORD_LONG_VALUE = 31;
  public static final byte CLUSTER_PAGE_SET_PREV_PAGE    = 32;

  public static final byte MAP_ENTRY_POINT_SET_FILE_SIZE = 33;

  public static final byte CLUSTER_STATE_V_ONE_SET_SIZE           = 36;
  public static final byte CLUSTER_STATE_V_ONE_SET_FILE_SIZE      = 37;
  public static final byte CLUSTER_STATE_V_ONE_SET_FREE_LIST_PAGE = 38;

  public static final byte SBTREE_BUCKET_SET_TREE_SIZE                  = 40;
  public static final byte SBTREE_BUCKET_SET_FREE_LIST_FIRST_INDEX      = 41;
  public static final byte SBTREE_BUCKET_REMOVE_LEAF_ENTRY              = 42;
  public static final byte SBTREE_BUCKET_ADD_ALL                        = 43;
  public static final byte SBTREE_BUCKET_SHRINK                         = 44;
  public static final byte SBTREE_BUCKET_INSERT_LEAF_KEY_VALUE          = 45;
  public static final byte SBTREE_BUCKET_INSERT_NON_LEAF_KEY_NEIGHBOURS = 46;
  public static final byte SBTREE_BUCKET_REMOVE_NON_LEAF_ENTRY          = 47;
  public static final byte SBTREE_BUCKET_UPDATE_VALUE                   = 48;
  public static final byte SBTREE_BUCKET_SET_LEFT_SIBLING               = 49;
  public static final byte SBTREE_BUCKET_SET_RIGHT_SIBLING              = 50;
  public static final byte SBTREE_BUCKET_CONVERT_TO_LEAF_PAGE           = 51;
  public static final byte SBTREE_BUCKET_CONVERT_TO_NON_LEAF_PAGE       = 52;

  public static final byte SBTREE_NULL_BUCKET_SET_VALUE    = 53;
  public static final byte SBTREE_NULL_BUCKET_REMOVE_VALUE = 54;

  public static final byte SBTREE_BONSAI_BUCKET_SET_TREE_SIZE         = 56;
  public static final byte SBTREE_BONSAI_BUCKET_REMOVE_LEAF_ENTRY     = 57;
  public static final byte SBTREE_BONSAI_BUCKET_CONVERT_TO_LEAF       = 58;
  public static final byte SBTREE_BONSAI_BUCKET_CONVERT_TO_NON_LEAF   = 59;
  public static final byte SBTREE_BONSAI_BUCKET_ADD_ALL               = 60;
  public static final byte SBTREE_BONSAI_BUCKET_SHRINK                = 61;
  public static final byte SBTREE_BONSAI_BUCKET_INSERT_LEAF_ENTRY     = 62;
  public static final byte SBTREE_BONSAI_BUCKET_INSERT_NON_LEAF_ENTRY = 63;
  public static final byte SBTREE_BONSAI_BUCKET_REMOVE_NON_LEAF_ENTRY = 64;
  public static final byte SBTREE_BONSAI_BUCKET_UPDATE_VALUE          = 65;
  public static final byte SBTREE_BONSAI_BUCKET_SET_FREE_LIST_POINTER = 66;
  public static final byte SBTREE_BONSAI_BUCKET_SET_DELETED           = 67;
  public static final byte SBTREE_BONSAI_BUCKET_SET_LEFT_SIBLING      = 68;
  public static final byte SBTREE_BONSAI_BUCKET_SET_RIGHT_SIBLING     = 69;

  public static final byte SBTREE_BONSAI_SYS_BUCKET_SET_FREE_LIST_LENGTH   = 71;
  public static final byte SBTREE_BONSAI_SYS_BUCKET_SET_FREE_SPACE_POINTER = 72;
  public static final byte SBTREE_BONSAI_SYS_BUCKET_SET_FREE_LIST_HEAD     = 73;

  public static final byte HASH_INDEX_BUCKET_UPDATE_ENTRY = 74;
  public static final byte HASH_INDEX_BUCKET_DELETE_ENTRY = 75;
  public static final byte HASH_INDEX_BUCKET_ADD_ENTRY    = 76;
  public static final byte HASH_INDEX_BUCKET_SET_DEPTH    = 77;
}
