package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.bteebonsai.bonsaibucket;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.OSBTreeBonsaiBucket;

public final class OBonsaiBucketConvertToNonLeafPageOperation extends OBonsaiBucketPageOperation {
  public OBonsaiBucketConvertToNonLeafPageOperation() {
  }

  public OBonsaiBucketConvertToNonLeafPageOperation(final int pageOffset) {
    super(pageOffset);
  }

  @Override
  public final byte getId() {
    return WALRecordTypes.SBTREE_BONSAI_BUCKET_CONVERT_TO_NON_LEAF;
  }

  @Override
  protected final void doUndo(final OSBTreeBonsaiBucket page) {
    page.convertToLeaf();
  }
}
