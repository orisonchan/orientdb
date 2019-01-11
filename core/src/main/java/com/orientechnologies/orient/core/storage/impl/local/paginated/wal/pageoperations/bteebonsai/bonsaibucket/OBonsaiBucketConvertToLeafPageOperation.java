package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.bteebonsai.bonsaibucket;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.OSBTreeBonsaiBucket;

public final class OBonsaiBucketConvertToLeafPageOperation extends OBonsaiBucketPageOperation {
  public OBonsaiBucketConvertToLeafPageOperation() {
  }

  public OBonsaiBucketConvertToLeafPageOperation(final int pageOffset) {
    super(pageOffset);
  }

  @Override
  public byte getId() {
    return WALRecordTypes.SBTREE_BONSAI_BUCKET_CONVERT_TO_LEAF;
  }

  @Override
  protected void doUndo(final OSBTreeBonsaiBucket page) {
    page.convertToNonLeaf();
  }
}
