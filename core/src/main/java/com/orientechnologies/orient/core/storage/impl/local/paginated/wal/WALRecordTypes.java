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

  public static final byte CLUSTER_POSITION_MAP_NEW_PAGE       = 15;
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

  public static final byte CLUSTER_PAGE_NEW              = 26;
  public static final byte CLUSTER_PAGE_APPEND_RECORD    = 27;
  public static final byte CLUSTER_PAGE_REPLACE_RECORD   = 28;
  public static final byte CLUSTER_PAGE_DELETE_RECORD    = 29;
  public static final byte CLUSTER_PAGE_SET_NEXT_PAGE    = 30;
  public static final byte CLUSTER_SET_RECORD_LONG_VALUE = 31;
  public static final byte CLUSTER_PAGE_SET_PREV_PAGE    = 32;

  public static final byte MAP_ENTRY_POINT_SET_FILE_SIZE = 33;
  public static final byte MAP_ENTRY_POINT_NEW_PAGE      = 34;

}
