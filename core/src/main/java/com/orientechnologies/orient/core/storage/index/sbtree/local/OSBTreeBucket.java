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

package com.orientechnologies.orient.core.storage.index.sbtree.local;

import com.orientechnologies.common.comparator.ODefaultComparator;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.encryption.OEncryption;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;
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
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 8/7/13
 */
public final class OSBTreeBucket<K, V> extends ODurablePage {
  private static final int FREE_POINTER_OFFSET  = NEXT_FREE_POSITION;
  private static final int SIZE_OFFSET          = FREE_POINTER_OFFSET + OIntegerSerializer.INT_SIZE;
  private static final int IS_LEAF_OFFSET       = SIZE_OFFSET + OIntegerSerializer.INT_SIZE;
  private static final int LEFT_SIBLING_OFFSET  = IS_LEAF_OFFSET + OByteSerializer.BYTE_SIZE;
  private static final int RIGHT_SIBLING_OFFSET = LEFT_SIBLING_OFFSET + OLongSerializer.LONG_SIZE;

  private static final int TREE_SIZE_OFFSET = RIGHT_SIBLING_OFFSET + OLongSerializer.LONG_SIZE;

  /**
   * KEY_SERIALIZER_OFFSET and VALUE_SERIALIZER_OFFSET are no longer used by sb-tree since 1.7.
   * However we left them in buckets to support backward compatibility.
   */
  private static final int KEY_SERIALIZER_OFFSET   = TREE_SIZE_OFFSET + OLongSerializer.LONG_SIZE;
  private static final int VALUE_SERIALIZER_OFFSET = KEY_SERIALIZER_OFFSET + OByteSerializer.BYTE_SIZE;

  private static final int FREE_VALUES_LIST_OFFSET = VALUE_SERIALIZER_OFFSET + OByteSerializer.BYTE_SIZE;

  private static final int POSITIONS_ARRAY_OFFSET = FREE_VALUES_LIST_OFFSET + OLongSerializer.LONG_SIZE;

  private boolean isLeaf;

  private final Comparator<? super K> comparator = ODefaultComparator.INSTANCE;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  OSBTreeBucket(final OCacheEntry cacheEntry, final boolean isLeaf) {
    super(cacheEntry);

    this.isLeaf = isLeaf;

    setIntValue(FREE_POINTER_OFFSET, MAX_PAGE_SIZE_BYTES);
    setIntValue(SIZE_OFFSET, 0);

    setByteValue(IS_LEAF_OFFSET, (byte) (isLeaf ? 1 : 0));
    setLongValue(LEFT_SIBLING_OFFSET, -1);
    setLongValue(RIGHT_SIBLING_OFFSET, -1);

    setLongValue(TREE_SIZE_OFFSET, 0);
    setLongValue(FREE_VALUES_LIST_OFFSET, -1);
  }

  public OSBTreeBucket(final OCacheEntry cacheEntry, final boolean isLeaf, final byte keySerializerId,
      final byte valueSerializerId) {
    super(cacheEntry);

    this.isLeaf = isLeaf;

    setIntValue(FREE_POINTER_OFFSET, MAX_PAGE_SIZE_BYTES);
    setIntValue(SIZE_OFFSET, 0);

    setByteValue(IS_LEAF_OFFSET, (byte) (isLeaf ? 1 : 0));
    setLongValue(LEFT_SIBLING_OFFSET, -1);
    setLongValue(RIGHT_SIBLING_OFFSET, -1);

    setLongValue(TREE_SIZE_OFFSET, 0);
    setLongValue(FREE_VALUES_LIST_OFFSET, -1);

    setByteValue(KEY_SERIALIZER_OFFSET, keySerializerId);
    setByteValue(VALUE_SERIALIZER_OFFSET, valueSerializerId);
  }

  public OSBTreeBucket(final OCacheEntry cacheEntry) {
    super(cacheEntry);
    this.isLeaf = getByteValue(IS_LEAF_OFFSET) > 0;
  }

  public void setTreeSize(final long size) {
    final int prevSize = (int) getLongValue(TREE_SIZE_OFFSET);
    setLongValue(TREE_SIZE_OFFSET, size);

    addPageOperation(new OSBTreeBucketSetTreeSizePageOperation(prevSize));
  }

  long getTreeSize() {
    return getLongValue(TREE_SIZE_OFFSET);
  }

  public boolean isEmpty() {
    return size() == 0;
  }

  long getValuesFreeListFirstIndex() {
    return getLongValue(FREE_VALUES_LIST_OFFSET);
  }

  public void convertToNonLeafPage() {
    assert isLeaf;

    isLeaf = false;
    setByteValue(IS_LEAF_OFFSET, (byte) 0);
    addPageOperation(new OSBTreeBucketConvertToNonLeafPageOperation());
  }

  public void convertToLeafPage() {
    assert !isLeaf;

    isLeaf = true;
    setByteValue(IS_LEAF_OFFSET, (byte) 1);
    addPageOperation(new OSBTreeBucketConvertToLeafPageOperation());
  }

  public void setValuesFreeListFirstIndex(final long pageIndex) {
    final int prevPageIndex = (int) getLongValue(FREE_VALUES_LIST_OFFSET);
    setLongValue(FREE_VALUES_LIST_OFFSET, pageIndex);

    addPageOperation(new OSBtreeBucketSetFreeListFirstIndexPageOperation(prevPageIndex));
  }

  public int find(final K key, final OEncryption encryption, final OBinarySerializer<K> keySerializer) {
    int low = 0;
    int high = size() - 1;

    while (low <= high) {
      final int mid = (low + high) >>> 1;
      final K midVal = getKey(mid, keySerializer, encryption);
      final int cmp = comparator.compare(midVal, key);

      if (cmp < 0)
        low = mid + 1;
      else if (cmp > 0)
        high = mid - 1;
      else
        return mid; // key found
    }
    return -(low + 1); // key not found.
  }

  public void removeLeafEntry(final int entryIndex, final int keySize, final int valueSize) {
    final int entryPosition = getIntValue(POSITIONS_ARRAY_OFFSET + entryIndex * OIntegerSerializer.INT_SIZE);

    assert isLeaf;

    final int entrySize = keySize + valueSize + OByteSerializer.BYTE_SIZE;
    final byte[] key = getBinaryValue(entryPosition, keySize);
    final byte[] value = getBinaryValue(entryPosition + OByteSerializer.BYTE_SIZE + keySize, valueSize);

    int size = getIntValue(SIZE_OFFSET);
    if (entryIndex < size - 1) {
      moveData(POSITIONS_ARRAY_OFFSET + (entryIndex + 1) * OIntegerSerializer.INT_SIZE,
          POSITIONS_ARRAY_OFFSET + entryIndex * OIntegerSerializer.INT_SIZE, (size - entryIndex - 1) * OIntegerSerializer.INT_SIZE);
    }

    size--;
    setIntValue(SIZE_OFFSET, size);

    final int freePointer = getIntValue(FREE_POINTER_OFFSET);
    if (size > 0 && entryPosition > freePointer) {
      moveData(freePointer, freePointer + entrySize, entryPosition - freePointer);
    }

    setIntValue(FREE_POINTER_OFFSET, freePointer + entrySize);

    int currentPositionOffset = POSITIONS_ARRAY_OFFSET;

    for (int i = 0; i < size; i++) {
      final int currentEntryPosition = getIntValue(currentPositionOffset);
      if (currentEntryPosition < entryPosition) {
        setIntValue(currentPositionOffset, currentEntryPosition + entrySize);
      }
      currentPositionOffset += OIntegerSerializer.INT_SIZE;
    }

    addPageOperation(new OSBTreeBucketRemoveLeafEntryPageOperation(entryIndex, key, value));
  }

  public void removeNonLeafEntry(final int index, final int keySize, final int prevChildPointer) {
    final int entryPosition = getIntValue(POSITIONS_ARRAY_OFFSET + index * OIntegerSerializer.INT_SIZE);
    final int entrySize = keySize + 2 * OLongSerializer.LONG_SIZE;
    final byte[] key = getBinaryValue(entryPosition, keySize);

    int size = getIntValue(SIZE_OFFSET);
    if (index < size - 1) {
      moveData(POSITIONS_ARRAY_OFFSET + (index + 1) * OIntegerSerializer.INT_SIZE,
          POSITIONS_ARRAY_OFFSET + index * OIntegerSerializer.INT_SIZE, (size - index - 1) * OIntegerSerializer.INT_SIZE);
    }

    size--;
    setIntValue(SIZE_OFFSET, size);

    final int freePointer = getIntValue(FREE_POINTER_OFFSET);
    if (size > 0 && entryPosition > freePointer) {
      moveData(freePointer, freePointer + entrySize, entryPosition - freePointer);
    }

    setIntValue(FREE_POINTER_OFFSET, freePointer + entrySize);

    int currentPositionOffset = POSITIONS_ARRAY_OFFSET;

    for (int i = 0; i < size; i++) {
      final int currentEntryPosition = getIntValue(currentPositionOffset);
      if (currentEntryPosition < entryPosition) {
        setIntValue(currentPositionOffset, currentEntryPosition + entrySize);
      }
      currentPositionOffset += OIntegerSerializer.INT_SIZE;
    }

    int leftNeighbour = -1;
    int rightNeighbour = -1;
    if (index > 0 && prevChildPointer >= 0) {
      final int prevEntryPosition = getIntValue(POSITIONS_ARRAY_OFFSET + (index - 1) * OIntegerSerializer.INT_SIZE);
      leftNeighbour = (int) getLongValue(prevEntryPosition + OLongSerializer.LONG_SIZE);
      setLongValue(prevEntryPosition + OLongSerializer.LONG_SIZE, prevChildPointer);
    }

    if (index <= size - 1 && prevChildPointer >= 0) {
      final int nextEntryPosition = getIntValue(POSITIONS_ARRAY_OFFSET + index * OIntegerSerializer.INT_SIZE);
      rightNeighbour = (int) getLongValue(nextEntryPosition);
      setLongValue(nextEntryPosition, prevChildPointer);
    }

    addPageOperation(new OSBTreeBucketRemoveNonLeafEntryPageOperation(index, key, prevChildPointer, leftNeighbour, rightNeighbour));
  }

  public int size() {
    return getIntValue(SIZE_OFFSET);
  }

  public SBTreeEntry<K, V> getEntry(final int entryIndex, final OEncryption encryption, final OBinarySerializer<K> keySerializer,
      final OBinarySerializer<V> valueSerializer) {
    int entryPosition = getIntValue(entryIndex * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET);

    if (isLeaf) {
      final K key;
      if (encryption == null) {
        key = deserializeFromDirectMemory(keySerializer, entryPosition);

        entryPosition += getObjectSizeInDirectMemory(keySerializer, entryPosition);
      } else {
        final int encryptionSize = getIntValue(entryPosition);
        entryPosition += OIntegerSerializer.INT_SIZE;

        final byte[] encryptedKey = getBinaryValue(entryPosition, encryptionSize);
        entryPosition += encryptedKey.length;

        final byte[] serializedKey = encryption.decrypt(encryptedKey);

        key = keySerializer.deserializeNativeObject(serializedKey, 0);
      }

      final boolean isLinkValue = getByteValue(entryPosition) > 0;
      long link = -1;
      V value = null;

      if (isLinkValue)
        link = deserializeFromDirectMemory(OLongSerializer.INSTANCE, entryPosition + OByteSerializer.BYTE_SIZE);
      else
        value = deserializeFromDirectMemory(valueSerializer, entryPosition + OByteSerializer.BYTE_SIZE);

      return new SBTreeEntry<>(-1, -1, key, new OSBTreeValue<>(link >= 0, link, value));
    } else {
      final long leftChild = getLongValue(entryPosition);
      entryPosition += OLongSerializer.LONG_SIZE;

      final long rightChild = getLongValue(entryPosition);
      entryPosition += OLongSerializer.LONG_SIZE;

      final K key;

      if (encryption == null) {
        key = deserializeFromDirectMemory(keySerializer, entryPosition);
      } else {
        final int encryptionSize = getIntValue(entryPosition);
        entryPosition += OIntegerSerializer.INT_SIZE;

        final byte[] encryptedKey = getBinaryValue(entryPosition, encryptionSize);

        final byte[] serializedKey = encryption.decrypt(encryptedKey);

        key = keySerializer.deserializeNativeObject(serializedKey, 0);
      }

      return new SBTreeEntry<>(leftChild, rightChild, key, null);
    }
  }

  byte[] getRawEntry(final int entryIndex, final boolean isEncrypted, final OBinarySerializer<K> keySerializer,
      final OBinarySerializer<V> valueSerializer) {
    int entryPosition = getIntValue(entryIndex * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET);
    final int startEntryPosition = entryPosition;

    if (isLeaf) {
      final int keySize;
      if (!isEncrypted) {
        keySize = getObjectSizeInDirectMemory(keySerializer, entryPosition);
      } else {
        final int encryptedSize = getIntValue(entryPosition);
        keySize = OIntegerSerializer.INT_SIZE + encryptedSize;
      }

      entryPosition += keySize;

      assert getByteValue(entryPosition) == 0;

      final int valueSize = getObjectSizeInDirectMemory(valueSerializer, entryPosition + OByteSerializer.BYTE_SIZE);

      return getBinaryValue(startEntryPosition, keySize + valueSize + OByteSerializer.BYTE_SIZE);
    } else {
      entryPosition += 2 * OLongSerializer.LONG_SIZE;

      final int keySize;
      if (!isEncrypted) {
        keySize = getObjectSizeInDirectMemory(keySerializer, entryPosition);
      } else {
        final int encryptedSize = getIntValue(entryPosition);
        keySize = OIntegerSerializer.INT_SIZE + encryptedSize;
      }

      return getBinaryValue(startEntryPosition, keySize + 2 * OLongSerializer.LONG_SIZE);
    }
  }

  /**
   * Obtains the value stored under the given entry index in this bucket.
   *
   * @param entryIndex the value entry index.
   *
   * @return the obtained value.
   */
  public OSBTreeValue<V> getValue(final int entryIndex, final OBinarySerializer<K> keySerializer,
      final OBinarySerializer<V> valueSerializer, final OEncryption encryption) {
    assert isLeaf;

    int entryPosition = getIntValue(entryIndex * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET);

    // skip key
    if (encryption == null) {
      entryPosition += getObjectSizeInDirectMemory(keySerializer, entryPosition);
    } else {
      final int encryptedSize = getIntValue(entryPosition);
      entryPosition += OIntegerSerializer.INT_SIZE + encryptedSize;
    }

    final boolean isLinkValue = getByteValue(entryPosition) > 0;
    long link = -1;
    V value = null;

    if (isLinkValue)
      link = deserializeFromDirectMemory(OLongSerializer.INSTANCE, entryPosition + OByteSerializer.BYTE_SIZE);
    else
      value = deserializeFromDirectMemory(valueSerializer, entryPosition + OByteSerializer.BYTE_SIZE);

    return new OSBTreeValue<>(link >= 0, link, value);
  }

  byte[] getRawValue(final int entryIndex, final OBinarySerializer<K> keySerializer, final OBinarySerializer<V> valueSerializer,
      final OEncryption encryption) {
    assert isLeaf;

    int entryPosition = getIntValue(entryIndex * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET);

    // skip key
    if (encryption == null) {
      entryPosition += getObjectSizeInDirectMemory(keySerializer, entryPosition);
    } else {
      final int encryptedSize = getIntValue(entryPosition);
      entryPosition += OIntegerSerializer.INT_SIZE + encryptedSize;
    }

    assert getByteValue(entryPosition) == 0;

    final int valueSize = getObjectSizeInDirectMemory(valueSerializer, entryPosition + OByteSerializer.BYTE_SIZE);

    return getBinaryValue(entryPosition + OByteSerializer.BYTE_SIZE, valueSize);
  }

  byte[] getRawKey(final int entryIndex, final OBinarySerializer<K> keySerializer, final OEncryption encryption) {
    assert isLeaf;

    final int entryPosition = getIntValue(entryIndex * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET);

    final byte[] rawKey;
    if (encryption == null) {
      final int keySize = getObjectSizeInDirectMemory(keySerializer, entryPosition);
      rawKey = getBinaryValue(entryPosition, keySize);
    } else {
      final int encryptedSize = getIntValue(entryPosition);

      rawKey = getBinaryValue(entryPosition, OIntegerSerializer.INT_SIZE + encryptedSize);
    }

    return rawKey;
  }

  public K getKey(final int index, final OBinarySerializer<K> keySerializer, final OEncryption encryption) {
    int entryPosition = getIntValue(index * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET);

    if (!isLeaf)
      entryPosition += 2 * OLongSerializer.LONG_SIZE;

    if (encryption == null) {
      return deserializeFromDirectMemory(keySerializer, entryPosition);
    } else {
      final int encryptedSize = getIntValue(entryPosition);
      entryPosition += OIntegerSerializer.INT_SIZE;

      final byte[] encryptedKey = getBinaryValue(entryPosition, encryptedSize);
      final byte[] serializedKey = encryption.decrypt(encryptedKey);
      return keySerializer.deserializeNativeObject(serializedKey, 0);
    }
  }

  public boolean isLeaf() {
    return isLeaf;
  }

  public void addAll(final List<byte[]> rawEntries, final byte keySerializerId, final byte valuesSerializerId,
      final boolean isEncrypted) {
    final int size = size();

    for (int i = 0; i < rawEntries.size(); i++) {
      appendRawEntry(size + i, rawEntries.get(i));
    }

    setIntValue(SIZE_OFFSET, size + rawEntries.size());

    addPageOperation(new OSBTreeBucketAddAllPageOperation(keySerializerId, valuesSerializerId, isEncrypted, rawEntries.size()));
  }

  public void shrink(final int newSize, final OBinarySerializer<K> keySerializer, final OBinarySerializer<V> valueSerializer,
      final boolean isEncrypted) {
    final int size = size();

    final List<byte[]> rawEntries = new ArrayList<>(newSize);
    final List<byte[]> removedEntries = new ArrayList<>(size - newSize);

    for (int i = 0; i < newSize; i++) {
      rawEntries.add(getRawEntry(i, isEncrypted, keySerializer, valueSerializer));
    }
    for (int i = newSize; i < size; i++) {
      removedEntries.add(getRawEntry(i, isEncrypted, keySerializer, valueSerializer));
    }

    setIntValue(FREE_POINTER_OFFSET, MAX_PAGE_SIZE_BYTES);

    int index = 0;
    for (final byte[] entry : rawEntries) {
      appendRawEntry(index, entry);
      index++;
    }

    setIntValue(SIZE_OFFSET, newSize);

    addPageOperation(
        new OSBTreeBucketShrinkPageOperation(removedEntries, keySerializer.getId(), valueSerializer.getId(), isEncrypted));
  }

  public boolean insertLeafKeyValue(final int index, final byte[] serializedKey, final byte[] serializedValue) {
    final int entrySize = serializedKey.length + serializedValue.length + OByteSerializer.BYTE_SIZE;

    assert isLeaf;
    final int size = getIntValue(SIZE_OFFSET);

    int freePointer = getIntValue(FREE_POINTER_OFFSET);
    if (freePointer - entrySize < (size + 1) * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET) {
      return false;
    }

    if (index <= size - 1) {
      moveData(POSITIONS_ARRAY_OFFSET + index * OIntegerSerializer.INT_SIZE,
          POSITIONS_ARRAY_OFFSET + (index + 1) * OIntegerSerializer.INT_SIZE, (size - index) * OIntegerSerializer.INT_SIZE);
    }

    freePointer -= entrySize;

    setIntValue(FREE_POINTER_OFFSET, freePointer);
    setIntValue(POSITIONS_ARRAY_OFFSET + index * OIntegerSerializer.INT_SIZE, freePointer);
    setIntValue(SIZE_OFFSET, size + 1);

    setBinaryValue(freePointer, serializedKey);
    setByteValue(freePointer + serializedKey.length, (byte) 0);
    setBinaryValue(freePointer + serializedKey.length + OByteSerializer.BYTE_SIZE, serializedValue);

    addPageOperation(new OSBTreeBucketInsertLeafKeyValuePageOperation(index, serializedKey.length, serializedValue.length));

    return true;
  }

  public boolean insertNonLeafKeyNeighbours(final int index, final byte[] serializedKey, final int leftChild, final int rightChild,
      final boolean updateNeighbors) {
    final int entrySize = serializedKey.length + 2 * OLongSerializer.LONG_SIZE;

    int size = size();
    int freePointer = getIntValue(FREE_POINTER_OFFSET);
    if (freePointer - entrySize < (size + 1) * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET) {
      return false;
    }

    if (index <= size - 1) {
      moveData(POSITIONS_ARRAY_OFFSET + index * OIntegerSerializer.INT_SIZE,
          POSITIONS_ARRAY_OFFSET + (index + 1) * OIntegerSerializer.INT_SIZE, (size - index) * OIntegerSerializer.INT_SIZE);
    }

    freePointer -= entrySize;

    setIntValue(FREE_POINTER_OFFSET, freePointer);
    setIntValue(POSITIONS_ARRAY_OFFSET + index * OIntegerSerializer.INT_SIZE, freePointer);
    setIntValue(SIZE_OFFSET, size + 1);

    freePointer += setLongValue(freePointer, leftChild);
    freePointer += setLongValue(freePointer, rightChild);

    setBinaryValue(freePointer, serializedKey);
    size++;

    int prevChild = -1;
    if (updateNeighbors && size > 1) {
      if (index < size - 1) {
        final int nextEntryPosition = getIntValue(POSITIONS_ARRAY_OFFSET + (index + 1) * OIntegerSerializer.INT_SIZE);
        prevChild = (int) getLongValue(nextEntryPosition);
        setLongValue(nextEntryPosition, rightChild);
      }

      if (index > 0) {
        final int prevEntryPosition = getIntValue(POSITIONS_ARRAY_OFFSET + (index - 1) * OIntegerSerializer.INT_SIZE);
        assert prevChild < 0 || prevChild == getLongValue(prevEntryPosition + OLongSerializer.LONG_SIZE);
        setLongValue(prevEntryPosition + OLongSerializer.LONG_SIZE, leftChild);
      }
    }

    addPageOperation(new OSBTreeBucketInsertNonLeafKeyNeighboursPageOperation(index, serializedKey.length, prevChild));

    return true;
  }

  private void appendRawEntry(final int index, final byte[] rawEntry) {
    int freePointer = getIntValue(FREE_POINTER_OFFSET);
    freePointer -= rawEntry.length;

    setIntValue(FREE_POINTER_OFFSET, freePointer);
    setIntValue(POSITIONS_ARRAY_OFFSET + index * OIntegerSerializer.INT_SIZE, freePointer);

    setBinaryValue(freePointer, rawEntry);
  }

  boolean insertEntry(final int index, final SBTreeEntry<K, V> treeEntry, final OBinarySerializer<K> keySerializer,
      final OEncryption encryption, final OType[] keyTypes, final OBinarySerializer<V> valueSerializer) {
    final byte[] serializedKey = keySerializer.serializeNativeAsWhole(treeEntry.key, (Object[]) keyTypes);
    final byte[] rawKey;

    if (encryption == null) {
      rawKey = serializedKey;
    } else {
      final byte[] encryptedKey = encryption.encrypt(serializedKey);
      rawKey = new byte[OIntegerSerializer.INT_SIZE + encryptedKey.length];
      OIntegerSerializer.INSTANCE.serializeNative(encryptedKey.length, rawKey, 0);
      System.arraycopy(encryptedKey, 0, rawKey, OIntegerSerializer.INT_SIZE, encryptedKey.length);
    }

    if (isLeaf) {
      assert !treeEntry.value.isLink();
      final byte[] serializedValue = valueSerializer.serializeNativeAsWhole(treeEntry.value.getValue());

      return insertLeafKeyValue(index, rawKey, serializedValue);
    }

    return insertNonLeafKeyNeighbours(index, rawKey, (int) treeEntry.leftChild, (int) treeEntry.rightChild, true);
  }

  public void updateValue(final int index, final byte[] value, final OBinarySerializer<K> keySerializer,
      final boolean isEncrypted) {
    int entryPosition = getIntValue(index * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET);

    if (!isEncrypted) {
      entryPosition += getObjectSizeInDirectMemory(keySerializer, entryPosition);
    } else {
      final int encryptedValue = getIntValue(entryPosition);
      entryPosition += OIntegerSerializer.INT_SIZE + encryptedValue;
    }

    assert getByteValue(entryPosition) == 0;

    entryPosition += OByteSerializer.BYTE_SIZE;

    final byte[] prevValue = getBinaryValue(entryPosition, value.length);
    setBinaryValue(entryPosition, value);

    addPageOperation(new OSBTreeBucketUpdateValuePageOperation(index, prevValue, keySerializer.getId(), isEncrypted));
  }

  public void setLeftSibling(final long pageIndex) {
    final int prevLeftSibling = (int) getLongValue(LEFT_SIBLING_OFFSET);

    setLongValue(LEFT_SIBLING_OFFSET, pageIndex);
    addPageOperation(new OSBTreeBucketSetLeftSiblingPageOperation(prevLeftSibling));
  }

  long getLeftSibling() {
    return getLongValue(LEFT_SIBLING_OFFSET);
  }

  public void setRightSibling(final long pageIndex) {
    final int prevRightSibling = (int) getLongValue(RIGHT_SIBLING_OFFSET);
    setLongValue(RIGHT_SIBLING_OFFSET, pageIndex);

    addPageOperation(new OSBTreeBucketSetRightSiblingPageOperation((int) pageIndex, prevRightSibling));
  }

  long getRightSibling() {
    return getLongValue(RIGHT_SIBLING_OFFSET);
  }

  public static final class SBTreeEntry<K, V> implements Comparable<SBTreeEntry<K, V>> {
    private final Comparator<? super K> comparator = ODefaultComparator.INSTANCE;

    final        long            leftChild;
    final        long            rightChild;
    public final K               key;
    public final OSBTreeValue<V> value;

    public SBTreeEntry(final long leftChild, final long rightChild, final K key, final OSBTreeValue<V> value) {
      this.leftChild = leftChild;
      this.rightChild = rightChild;
      this.key = key;
      this.value = value;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o)
        return true;
      if (o == null || getClass() != o.getClass())
        return false;

      final SBTreeEntry<?, ?> that = (SBTreeEntry<?, ?>) o;

      if (leftChild != that.leftChild)
        return false;
      if (rightChild != that.rightChild)
        return false;
      if (!key.equals(that.key))
        return false;
      if (value != null) {
        return value.equals(that.value);
      } else {
        return that.value == null;
      }

    }

    @Override
    public int hashCode() {
      int result = (int) (leftChild ^ (leftChild >>> 32));
      result = 31 * result + (int) (rightChild ^ (rightChild >>> 32));
      result = 31 * result + key.hashCode();
      result = 31 * result + (value != null ? value.hashCode() : 0);
      return result;
    }

    @Override
    public String toString() {
      return "SBTreeEntry{" + "leftChild=" + leftChild + ", rightChild=" + rightChild + ", key=" + key + ", value=" + value + '}';
    }

    @Override
    public int compareTo(final SBTreeEntry<K, V> other) {
      return comparator.compare(key, other.key);
    }
  }
}
