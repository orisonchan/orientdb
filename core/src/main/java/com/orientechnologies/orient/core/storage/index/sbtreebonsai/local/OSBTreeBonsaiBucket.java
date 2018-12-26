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

import com.orientechnologies.common.comparator.ODefaultComparator;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 8/7/13
 */
public final class OSBTreeBonsaiBucket<K, V> extends OBonsaiBucketAbstract {
  static final         int     MAX_BUCKET_SIZE_BYTES    = OGlobalConfiguration.SBTREEBONSAI_BUCKET_SIZE.getValueAsInteger() * 1024;
  /**
   * Maximum size of key-value pair which can be put in SBTreeBonsai in bytes (24576000 by default)
   */
  private static final byte    LEAF                     = 0x1;
  private static final byte    DELETED                  = 0x2;
  private static final int     MAX_ENTREE_SIZE          = 24576000;
  private static final int     FREE_POINTER_OFFSET      = WAL_POSITION_OFFSET + OLongSerializer.LONG_SIZE;
  private static final int     SIZE_OFFSET              = FREE_POINTER_OFFSET + OIntegerSerializer.INT_SIZE;
  private static final int     FLAGS_OFFSET             = SIZE_OFFSET + OIntegerSerializer.INT_SIZE;
  private static final int     FREE_LIST_POINTER_OFFSET = FLAGS_OFFSET + OByteSerializer.BYTE_SIZE;
  private static final int     LEFT_SIBLING_OFFSET      = FREE_LIST_POINTER_OFFSET + OBonsaiBucketPointer.SIZE;
  private static final int     RIGHT_SIBLING_OFFSET     = LEFT_SIBLING_OFFSET + OBonsaiBucketPointer.SIZE;
  private static final int     TREE_SIZE_OFFSET         = RIGHT_SIBLING_OFFSET + OBonsaiBucketPointer.SIZE;
  private static final int     KEY_SERIALIZER_OFFSET    = TREE_SIZE_OFFSET + OLongSerializer.LONG_SIZE;
  private static final int     VALUE_SERIALIZER_OFFSET  = KEY_SERIALIZER_OFFSET + OByteSerializer.BYTE_SIZE;
  private static final int     POSITIONS_ARRAY_OFFSET   = VALUE_SERIALIZER_OFFSET + OByteSerializer.BYTE_SIZE;
  private              boolean isLeaf;
  private final        int     offset;

  private final Comparator<? super K> comparator = ODefaultComparator.INSTANCE;

  public static final class SBTreeEntry<K, V> implements Map.Entry<K, V>, Comparable<SBTreeEntry<K, V>> {
    final         OBonsaiBucketPointer  leftChild;
    final         OBonsaiBucketPointer  rightChild;
    public final  K                     key;
    public final  V                     value;
    private final Comparator<? super K> comparator = ODefaultComparator.INSTANCE;

    public SBTreeEntry(final OBonsaiBucketPointer leftChild, final OBonsaiBucketPointer rightChild, final K key, final V value) {
      this.leftChild = leftChild;
      this.rightChild = rightChild;
      this.key = key;
      this.value = value;
    }

    @Override
    public K getKey() {
      return key;
    }

    @Override
    public V getValue() {
      return value;
    }

    @Override
    public V setValue(final V value) {
      throw new UnsupportedOperationException("SBTreeEntry.setValue");
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o)
        return true;
      if (o == null || getClass() != o.getClass())
        return false;

      final SBTreeEntry that = (SBTreeEntry) o;

      if (!leftChild.equals(that.leftChild))
        return false;
      if (!rightChild.equals(that.rightChild))
        return false;
      if (!key.equals(that.key))
        return false;

      return Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
      int result = leftChild.hashCode();
      result = 31 * result + rightChild.hashCode();
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

  public OSBTreeBonsaiBucket(final OCacheEntry cacheEntry, final int pageOffset) {
    super(cacheEntry);

    this.offset = pageOffset;
    this.isLeaf = (getByteValue(offset + FLAGS_OFFSET) & LEAF) == LEAF;
  }

  public void init(final boolean isLeaf, final byte keySerializerId, final byte valueSerializerId) {
    setIntValue(offset + FREE_POINTER_OFFSET, MAX_BUCKET_SIZE_BYTES);
    setIntValue(offset + SIZE_OFFSET, 0);

    //THIS REMOVE ALSO THE EVENTUAL DELETED FLAG
    setByteValue(offset + FLAGS_OFFSET, (isLeaf ? LEAF : 0));
    setLongValue(offset + LEFT_SIBLING_OFFSET, -1);
    setLongValue(offset + RIGHT_SIBLING_OFFSET, -1);

    setLongValue(offset + TREE_SIZE_OFFSET, 0);

    setByteValue(offset + KEY_SERIALIZER_OFFSET, keySerializerId);
    setByteValue(offset + VALUE_SERIALIZER_OFFSET, valueSerializerId);
  }

  public byte getKeySerializerId() {
    return getByteValue(offset + KEY_SERIALIZER_OFFSET);
  }

  public byte getValueSerializerId() {
    return getByteValue(offset + VALUE_SERIALIZER_OFFSET);
  }

  long getTreeSize() {
    return getLongValue(offset + TREE_SIZE_OFFSET);
  }

  public void setTreeSize(final long size) {
    final int prevTreeSize = (int) getLongValue(offset + TREE_SIZE_OFFSET);
    setLongValue(offset + TREE_SIZE_OFFSET, size);
    addPageOperation(new OBonsaiBucketSetTreeSizePageOperation(offset, prevTreeSize));
  }

  public boolean isEmpty() {
    return size() == 0;
  }

  public int find(final K key, final OBinarySerializer<K> keySerializer) {
    int low = 0;
    int high = size() - 1;

    while (low <= high) {
      final int mid = (low + high) >>> 1;
      final K midVal = getKey(mid, keySerializer);
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
    assert isLeaf;

    final int entryPosition = getIntValue(offset + POSITIONS_ARRAY_OFFSET + entryIndex * OIntegerSerializer.INT_SIZE);
    final int entrySize = keySize + valueSize;
    final byte[] key = getBinaryValue(entryPosition, keySize);
    final byte[] value = getBinaryValue(entryPosition, valueSize);

    int size = size();
    if (entryIndex < size - 1) {
      moveData(offset + POSITIONS_ARRAY_OFFSET + (entryIndex + 1) * OIntegerSerializer.INT_SIZE,
          offset + POSITIONS_ARRAY_OFFSET + entryIndex * OIntegerSerializer.INT_SIZE,
          (size - entryIndex - 1) * OIntegerSerializer.INT_SIZE);
    }

    size--;
    setIntValue(offset + SIZE_OFFSET, size);

    final int freePointer = getIntValue(offset + FREE_POINTER_OFFSET);
    if (size > 0 && entryPosition > freePointer) {
      moveData(offset + freePointer, offset + freePointer + entrySize, entryPosition - freePointer);
    }
    setIntValue(offset + FREE_POINTER_OFFSET, freePointer + entrySize);

    int currentPositionOffset = offset + POSITIONS_ARRAY_OFFSET;

    for (int i = 0; i < size; i++) {
      final int currentEntryPosition = getIntValue(currentPositionOffset);
      if (currentEntryPosition < entryPosition)
        setIntValue(currentPositionOffset, currentEntryPosition + entrySize);
      currentPositionOffset += OIntegerSerializer.INT_SIZE;
    }

    addPageOperation(new OBonsaiBucketRemoveLeafEntryPageOperation(offset, entryIndex, key, value));
  }

  public void removeNonLeafEntry(final int entryIndex, final int keySize, final int prevChildPageIndex,
      final int prevChildPageOffset) {
    assert isLeaf;

    final int entryPosition = getIntValue(offset + POSITIONS_ARRAY_OFFSET + entryIndex * OIntegerSerializer.INT_SIZE);

    final int entrySize = keySize + 2 * (OIntegerSerializer.INT_SIZE + OLongSerializer.LONG_SIZE);
    final byte[] key = getBinaryValue(entryPosition, keySize);

    int size = size();
    if (entryIndex < size - 1) {
      moveData(offset + POSITIONS_ARRAY_OFFSET + (entryIndex + 1) * OIntegerSerializer.INT_SIZE,
          offset + POSITIONS_ARRAY_OFFSET + entryIndex * OIntegerSerializer.INT_SIZE,
          (size - entryIndex - 1) * OIntegerSerializer.INT_SIZE);
    }

    size--;
    setIntValue(offset + SIZE_OFFSET, size);

    final int freePointer = getIntValue(offset + FREE_POINTER_OFFSET);
    if (size > 0 && entryPosition > freePointer) {
      moveData(offset + freePointer, offset + freePointer + entrySize, entryPosition - freePointer);
    }
    setIntValue(offset + FREE_POINTER_OFFSET, freePointer + entrySize);

    int currentPositionOffset = offset + POSITIONS_ARRAY_OFFSET;

    for (int i = 0; i < size; i++) {
      final int currentEntryPosition = getIntValue(currentPositionOffset);
      if (currentEntryPosition < entryPosition)
        setIntValue(currentPositionOffset, currentEntryPosition + entrySize);
      currentPositionOffset += OIntegerSerializer.INT_SIZE;
    }

    int lefPageIndex = -1;
    int leftPageOffset = -1;

    int rightPageIndex = -1;
    int rightPageOffset = -1;

    if (prevChildPageIndex >= 0) {
      final OBonsaiBucketPointer prevChild = new OBonsaiBucketPointer(prevChildPageIndex, prevChildPageOffset);
      if (entryIndex < size) {
        final int nextEntryPosition = getIntValue(offset + POSITIONS_ARRAY_OFFSET + entryIndex * OIntegerSerializer.INT_SIZE);

        final OBonsaiBucketPointer leftChild = getBucketPointer(offset + nextEntryPosition);

        lefPageIndex = (int) leftChild.getPageIndex();
        leftPageOffset = leftChild.getPageOffset();

        setBucketPointer(offset + nextEntryPosition, prevChild);
      }

      if (entryIndex > 0) {
        final int prevEntryPosition = getIntValue(offset + POSITIONS_ARRAY_OFFSET + (entryIndex - 1) * OIntegerSerializer.INT_SIZE);

        final OBonsaiBucketPointer rightChild = getBucketPointer(
            offset + prevEntryPosition + OLongSerializer.LONG_SIZE + OIntegerSerializer.INT_SIZE);

        rightPageIndex = (int) rightChild.getPageIndex();
        rightPageOffset = rightChild.getPageOffset();

        setBucketPointer(offset + prevEntryPosition + OLongSerializer.LONG_SIZE + OIntegerSerializer.INT_SIZE, prevChild);
      }
    }

    addPageOperation(
        new OBonsaiBucketRemoveNonLeafEntryPageOperation(offset, entryIndex, key, lefPageIndex, leftPageOffset, rightPageIndex,
            rightPageOffset));
  }

  public int size() {
    return getIntValue(offset + SIZE_OFFSET);
  }

  public SBTreeEntry<K, V> getEntry(final int entryIndex, final OBinarySerializer<K> keySerializer,
      final OBinarySerializer<V> valueSerializer) {
    int entryPosition = getIntValue(offset + entryIndex * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET);

    if (isLeaf) {
      final K key = deserializeFromDirectMemory(keySerializer, offset + entryPosition);
      entryPosition += getObjectSizeInDirectMemory(keySerializer, offset + entryPosition);

      final V value = deserializeFromDirectMemory(valueSerializer, offset + entryPosition);

      return new SBTreeEntry<>(OBonsaiBucketPointer.NULL, OBonsaiBucketPointer.NULL, key, value);
    } else {
      final OBonsaiBucketPointer leftChild = getBucketPointer(offset + entryPosition);
      entryPosition += OBonsaiBucketPointer.SIZE;

      final OBonsaiBucketPointer rightChild = getBucketPointer(offset + entryPosition);
      entryPosition += OBonsaiBucketPointer.SIZE;

      final K key = deserializeFromDirectMemory(keySerializer, offset + entryPosition);

      return new SBTreeEntry<>(leftChild, rightChild, key, null);
    }
  }

  byte[] getRawEntry(final int entryIndex, final OBinarySerializer<K> keySerializer, final OBinarySerializer<V> valueSerializer) {
    int entryPosition = getIntValue(offset + entryIndex * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET);
    final int startEntryPosition = entryPosition;

    if (isLeaf) {
      final int keySize = getObjectSizeInDirectMemory(keySerializer, offset + entryPosition);
      entryPosition += keySize;

      final int valueSize = getObjectSizeInDirectMemory(valueSerializer, offset + entryPosition);

      return getBinaryValue(startEntryPosition + offset, keySize + valueSize);
    } else {
      entryPosition += 2 * OBonsaiBucketPointer.SIZE;
      final int keyLen = getObjectSizeInDirectMemory(keySerializer, offset + entryPosition);

      return getBinaryValue(startEntryPosition + offset, keyLen + 2 * OBonsaiBucketPointer.SIZE);
    }
  }

  @SuppressWarnings("SameParameterValue")
  byte[] getRawKey(final int entryIndex, final OBinarySerializer<K> keySerializer) {
    int entryPosition = getIntValue(offset + entryIndex * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET);
    final int startEntryPosition = entryPosition;

    if (isLeaf) {
      final int keySize = getObjectSizeInDirectMemory(keySerializer, offset + entryPosition);
      return getBinaryValue(offset + entryPosition, keySize);

    } else {
      entryPosition += 2 * OBonsaiBucketPointer.SIZE;
      final int keyLen = getObjectSizeInDirectMemory(keySerializer, offset + entryPosition);

      return getBinaryValue(startEntryPosition + offset, keyLen);
    }
  }

  @SuppressWarnings("SameParameterValue")
  byte[] getRawValue(final int entryIndex, final OBinarySerializer<K> keySerializer, final OBinarySerializer<V> valueSerializer) {
    assert isLeaf;

    int entryPosition = getIntValue(offset + entryIndex * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET);
    final int startEntryPosition = entryPosition;

    final int keySize = getObjectSizeInDirectMemory(keySerializer, offset + entryPosition);
    entryPosition += keySize;

    final int valueSize = getObjectSizeInDirectMemory(valueSerializer, offset + entryPosition);

    return getBinaryValue(startEntryPosition + offset + keySize, valueSize);
  }

  public K getKey(final int index, final OBinarySerializer<K> keySerializer) {
    int entryPosition = getIntValue(offset + index * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET);

    if (!isLeaf)
      entryPosition += 2 * (OLongSerializer.LONG_SIZE + OIntegerSerializer.INT_SIZE);

    return deserializeFromDirectMemory(keySerializer, offset + entryPosition);
  }

  public boolean isLeaf() {
    return isLeaf;
  }

  public void convertToLeaf() {
    assert !isLeaf;

    isLeaf = true;
    setByteValue(offset + FLAGS_OFFSET, (byte) 1);
    addPageOperation(new OBonsaiBucketConvertToLeafPageOperation(offset));
  }

  public void convertToNonLeaf() {
    assert isLeaf;

    isLeaf = false;
    setByteValue(offset + FLAGS_OFFSET, (byte) 0);
    addPageOperation(new OBonsaiBucketConvertToNonLeafPageOperation(offset));
  }

  public void addAll(final List<byte[]> entries) {
    final int size = size();

    for (int i = 0; i < entries.size(); i++) {
      appendRawEntry(size + i, entries.get(i));
    }

    setIntValue(offset + SIZE_OFFSET, size + entries.size());
    addPageOperation(new OBonsaiBucketAddAllPageOperation(offset, entries.size()));
  }

  public void shrink(final int newSize, final OBinarySerializer<K> keySerializer, final OBinarySerializer<V> valueSerializer) {
    final int size = size();
    final List<byte[]> rawEntries = new ArrayList<>(newSize);
    final List<byte[]> removedEntries = new ArrayList<>(size - newSize);

    for (int i = 0; i < newSize; i++) {
      rawEntries.add(getRawEntry(i, keySerializer, valueSerializer));
    }

    for (int i = newSize; i < size; i++) {
      removedEntries.add(getRawEntry(i + size, keySerializer, valueSerializer));
    }

    setIntValue(offset + FREE_POINTER_OFFSET, MAX_BUCKET_SIZE_BYTES);

    int index = 0;
    for (final byte[] entry : rawEntries) {
      appendRawEntry(index, entry);
      index++;
    }

    setIntValue(offset + SIZE_OFFSET, newSize);
    addPageOperation(new OBonsaiBucketShrinkPageOperation(offset, removedEntries));
  }

  boolean insertEntry(final int index, final SBTreeEntry<K, V> treeEntry, final OBinarySerializer<K> keySerializer,
      final OBinarySerializer<V> valueSerializer) {
    final byte[] key = keySerializer.serializeNativeAsWhole(treeEntry.key);
    if (isLeaf) {
      final byte[] value = valueSerializer.serializeNativeAsWhole(treeEntry.value);
      return insertLeafEntry(index, key, value);
    }

    return insertNonLeafEntry(index, key, treeEntry.leftChild, treeEntry.rightChild, true);
  }

  public boolean insertLeafEntry(final int index, final byte[] serializedKey, final byte[] serializedValue) {
    final int entrySize = serializedKey.length + serializedValue.length;
    checkEntreeSize(entrySize);

    final int size = size();
    int freePointer = getIntValue(offset + FREE_POINTER_OFFSET);
    if (freePointer - entrySize < (size + 1) * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET) {
      if (size > 1)
        return false;
      else
        throw new IllegalStateException(
            "Entry size ('key + value') is more than is more than allowed " + (freePointer - 2 * OIntegerSerializer.INT_SIZE
                + POSITIONS_ARRAY_OFFSET) + " bytes, either increase page size using '"
                + OGlobalConfiguration.SBTREEBONSAI_BUCKET_SIZE.getKey() + "' parameter, or decrease 'key + value' size.");
    }

    if (index <= size - 1) {
      moveData(offset + POSITIONS_ARRAY_OFFSET + index * OIntegerSerializer.INT_SIZE,
          offset + POSITIONS_ARRAY_OFFSET + (index + 1) * OIntegerSerializer.INT_SIZE,
          (size - index) * OIntegerSerializer.INT_SIZE);
    }

    freePointer -= entrySize;

    setIntValue(offset + FREE_POINTER_OFFSET, freePointer);
    setIntValue(offset + POSITIONS_ARRAY_OFFSET + index * OIntegerSerializer.INT_SIZE, freePointer);
    setIntValue(offset + SIZE_OFFSET, size + 1);

    setBinaryValue(offset + freePointer, serializedKey);
    freePointer += serializedKey.length;

    setBinaryValue(offset + freePointer, serializedValue);

    addPageOperation(new OBonsaiBucketInsertLeafEntryPageOperation(offset, index, serializedKey.length, serializedValue.length));

    return true;
  }

  public boolean insertNonLeafEntry(final int index, final byte[] serializedKey, final OBonsaiBucketPointer leftChild,
      final OBonsaiBucketPointer rightChild, final boolean updateNeighbors) {
    final int entrySize = serializedKey.length + 2 * (OLongSerializer.LONG_SIZE + OIntegerSerializer.INT_SIZE);

    int size = size();
    int freePointer = getIntValue(offset + FREE_POINTER_OFFSET);
    if (freePointer - entrySize < (size + 1) * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET) {
      if (size > 1)
        return false;
      else
        throw new IllegalStateException(
            "Entry size ('key + value') is more than is more than allowed " + (freePointer - 2 * OIntegerSerializer.INT_SIZE
                + POSITIONS_ARRAY_OFFSET) + " bytes, either increase page size using '"
                + OGlobalConfiguration.SBTREEBONSAI_BUCKET_SIZE.getKey() + "' parameter, or decrease 'key + value' size.");
    }

    if (index <= size - 1) {
      moveData(offset + POSITIONS_ARRAY_OFFSET + index * OIntegerSerializer.INT_SIZE,
          offset + POSITIONS_ARRAY_OFFSET + (index + 1) * OIntegerSerializer.INT_SIZE,
          (size - index) * OIntegerSerializer.INT_SIZE);
    }

    freePointer -= entrySize;

    setIntValue(offset + FREE_POINTER_OFFSET, freePointer);
    setIntValue(offset + POSITIONS_ARRAY_OFFSET + index * OIntegerSerializer.INT_SIZE, freePointer);
    setIntValue(offset + SIZE_OFFSET, size + 1);

    setBucketPointer(offset + freePointer, leftChild);
    freePointer += OLongSerializer.LONG_SIZE + OIntegerSerializer.INT_SIZE;

    setBucketPointer(offset + freePointer, rightChild);
    freePointer += OLongSerializer.LONG_SIZE + OIntegerSerializer.INT_SIZE;

    setBinaryValue(offset + freePointer, serializedKey);

    size++;

    int prevChildPageIndex = -1;
    int prevChildPageOffset = -1;

    if (updateNeighbors && size > 1) {
      if (index < size - 1) {
        final int nextEntryPosition = getIntValue(offset + POSITIONS_ARRAY_OFFSET + (index + 1) * OIntegerSerializer.INT_SIZE);
        final OBonsaiBucketPointer bucketPointer = getBucketPointer(offset + nextEntryPosition);

        prevChildPageIndex = (int) bucketPointer.getPageIndex();
        prevChildPageOffset = bucketPointer.getPageOffset();

        setBucketPointer(offset + nextEntryPosition, rightChild);
      }

      if (index > 0) {
        final int prevEntryPosition = getIntValue(offset + POSITIONS_ARRAY_OFFSET + (index - 1) * OIntegerSerializer.INT_SIZE);
        final OBonsaiBucketPointer bucketPointer = getBucketPointer(
            offset + prevEntryPosition + OLongSerializer.LONG_SIZE + OIntegerSerializer.INT_SIZE);

        prevChildPageIndex = (int) bucketPointer.getPageIndex();
        prevChildPageOffset = bucketPointer.getPageOffset();

        setBucketPointer(offset + prevEntryPosition + OLongSerializer.LONG_SIZE + OIntegerSerializer.INT_SIZE, leftChild);
      }
    }

    addPageOperation(new OBonsaiBucketInsertNonLeafEntryPageOperation(offset, index, serializedKey.length, prevChildPageIndex,
        prevChildPageOffset));

    return true;
  }

  private void appendRawEntry(final int index, final byte[] rawEntry) {
    int freePointer = getIntValue(offset + FREE_POINTER_OFFSET);

    freePointer -= rawEntry.length;

    setIntValue(offset + FREE_POINTER_OFFSET, freePointer);
    setIntValue(offset + POSITIONS_ARRAY_OFFSET + index * OIntegerSerializer.INT_SIZE, freePointer);

    setBinaryValue(offset + freePointer, rawEntry);
  }

  public void updateValue(final int index, final byte[] value, final OBinarySerializer<K> keySerializer) {
    int entryPosition = getIntValue(offset + index * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET);
    entryPosition += getObjectSizeInDirectMemory(keySerializer, offset + entryPosition);

    final int size = value.length;

    final byte[] prevValue = getBinaryValue(offset + entryPosition, size);
    setBinaryValue(offset + entryPosition, value);

    addPageOperation(new OBonsaiBucketUpdateValuePageOperation(offset, index, prevValue));
  }

  OBonsaiBucketPointer getFreeListPointer() {
    return getBucketPointer(offset + FREE_LIST_POINTER_OFFSET);
  }

  public void setFreeListPointer(final OBonsaiBucketPointer pointer) {
    final OBonsaiBucketPointer prevPointer = getBucketPointer(offset + FREE_LIST_POINTER_OFFSET);

    setBucketPointer(offset + FREE_LIST_POINTER_OFFSET, pointer);
    addPageOperation(
        new OBonsaiBucketSetFreeListPointerPageOperation(offset, (int) prevPointer.getPageIndex(), prevPointer.getPageOffset()));
  }

  void setDeleted() {
    assert size() == 0;

    final byte keySerializerId = getByteValue(KEY_SERIALIZER_OFFSET);
    final byte valueSerializerId = getByteValue(VALUE_SERIALIZER_OFFSET);
    final boolean isLeaf = (getByteValue(FLAGS_OFFSET) & LEAF) == 1;

    final byte value = getByteValue(offset + FLAGS_OFFSET);
    setByteValue(offset + FLAGS_OFFSET, (byte) (value | DELETED));

    addPageOperation(new OBonsaiBucketSetDeletedPageOperation(offset, keySerializerId, valueSerializerId, isLeaf));
  }

  public boolean isDeleted() {
    return (getByteValue(offset + FLAGS_OFFSET) & DELETED) == DELETED;
  }

  OBonsaiBucketPointer getLeftSibling() {
    return getBucketPointer(offset + LEFT_SIBLING_OFFSET);
  }

  public void setLeftSibling(final OBonsaiBucketPointer pointer) {
    final OBonsaiBucketPointer prevPointer = getBucketPointer(offset + LEFT_SIBLING_OFFSET);

    setBucketPointer(offset + LEFT_SIBLING_OFFSET, pointer);

    addPageOperation(
        new OBonsaiBucketSetLeftSiblingPageOperation(offset, (int) prevPointer.getPageIndex(), prevPointer.getPageOffset()));
  }

  OBonsaiBucketPointer getRightSibling() {
    return getBucketPointer(offset + RIGHT_SIBLING_OFFSET);
  }

  public void setRightSibling(final OBonsaiBucketPointer pointer) {
    final OBonsaiBucketPointer prevBucket = getBucketPointer(offset + RIGHT_SIBLING_OFFSET);

    setBucketPointer(offset + RIGHT_SIBLING_OFFSET, pointer);

    addPageOperation(
        new OBonsaiBucketSetRightSiblingPageOperation(offset, (int) prevBucket.getPageIndex(), prevBucket.getPageOffset()));
  }

  private static void checkEntreeSize(final int entreeSize) {
    if (entreeSize > MAX_ENTREE_SIZE)
      throw new IllegalStateException(
          "Serialized key-value pair size bigger than allowed " + entreeSize + " vs " + MAX_ENTREE_SIZE + ".");
  }
}
