package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.cluster.clusterstatevone;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class OClusterStateVOneSetFreeListPageOperationSerializationTest {
  @Test
  public void testStreamSerialization() {
    final int fileId = 34;
    final int pageIndex = 89;
    final OOperationUnitId operationUnitId = OOperationUnitId.generateId();

    final int index = 54;
    final int freeListPage = 23;
    final int oldFreeListPage = 12;

    OClusterStateVOneSetFreeListPageOperation operation = new OClusterStateVOneSetFreeListPageOperation(index, freeListPage,
        oldFreeListPage);

    operation.setFileId(fileId);
    operation.setPageIndex(pageIndex);
    operation.setOperationUnitId(operationUnitId);

    final int serializedSize = operation.serializedSize();
    final byte[] stream = new byte[serializedSize + 1];
    int offset = operation.toStream(stream, 1);

    Assert.assertEquals(serializedSize + 1, offset);

    OClusterStateVOneSetFreeListPageOperation restoredOperation = new OClusterStateVOneSetFreeListPageOperation();
    offset = restoredOperation.fromStream(stream, 1);

    Assert.assertEquals(serializedSize + 1, offset);
    Assert.assertEquals(fileId, restoredOperation.getFileId());
    Assert.assertEquals(pageIndex, restoredOperation.getPageIndex());
    Assert.assertEquals(operationUnitId, restoredOperation.getOperationUnitId());
    Assert.assertEquals(index, restoredOperation.getIndex());
    Assert.assertEquals(freeListPage, restoredOperation.getFreeListPage());
    Assert.assertEquals(oldFreeListPage, restoredOperation.getOldFreeListPage());
  }

  @Test
  public void testBufferSerialization() {
    final int fileId = 34;
    final int pageIndex = 89;
    final OOperationUnitId operationUnitId = OOperationUnitId.generateId();

    final int index = 54;
    final int freeListPage = 23;
    final int oldFreeListPage = 12;

    OClusterStateVOneSetFreeListPageOperation operation = new OClusterStateVOneSetFreeListPageOperation(index, freeListPage,
        oldFreeListPage);

    operation.setFileId(fileId);
    operation.setPageIndex(pageIndex);
    operation.setOperationUnitId(operationUnitId);

    final int serializedSize = operation.serializedSize();
    final ByteBuffer buffer = ByteBuffer.allocate(serializedSize + 1).order(ByteOrder.nativeOrder());
    buffer.position(1);

    operation.toStream(buffer);

    Assert.assertEquals(serializedSize + 1, buffer.position());

    OClusterStateVOneSetFreeListPageOperation restoredOperation = new OClusterStateVOneSetFreeListPageOperation();
    int offset = restoredOperation.fromStream(buffer.array(), 1);

    Assert.assertEquals(serializedSize + 1, offset);
    Assert.assertEquals(fileId, restoredOperation.getFileId());
    Assert.assertEquals(pageIndex, restoredOperation.getPageIndex());
    Assert.assertEquals(operationUnitId, restoredOperation.getOperationUnitId());
    Assert.assertEquals(index, restoredOperation.getIndex());
    Assert.assertEquals(freeListPage, restoredOperation.getFreeListPage());
    Assert.assertEquals(oldFreeListPage, restoredOperation.getOldFreeListPage());
  }
}
