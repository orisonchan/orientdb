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

import com.orientechnologies.common.comparator.ODefaultComparator;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.encryption.OEncryption;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.extendiblehashing.hashindexbucket.OHashIndexBucketAddEntryPageOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.extendiblehashing.hashindexbucket.OHashIndexBucketDeleteEntryPageOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.extendiblehashing.hashindexbucket.OHashIndexBucketSetDepthPageOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.extendiblehashing.hashindexbucket.OHashIndexBucketUpdateEntryPageOperation;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 2/17/13
 */
public final class OHashIndexBucket<K, V> extends ODurablePage {
  private static final int FREE_POINTER_OFFSET = NEXT_FREE_POSITION;
  private static final int DEPTH_OFFSET        = FREE_POINTER_OFFSET + OIntegerSerializer.INT_SIZE;
  private static final int SIZE_OFFSET         = DEPTH_OFFSET + OByteSerializer.BYTE_SIZE;
  private static final int HISTORY_OFFSET      = SIZE_OFFSET + OIntegerSerializer.INT_SIZE;

  private static final int NEXT_REMOVED_BUCKET_OFFSET = HISTORY_OFFSET + OLongSerializer.LONG_SIZE * 64;
  private static final int POSITIONS_ARRAY_OFFSET     = NEXT_REMOVED_BUCKET_OFFSET + OLongSerializer.LONG_SIZE;

  private static final int MAX_BUCKET_SIZE_BYTES = OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * 1024;

  private final Comparator keyComparator = ODefaultComparator.INSTANCE;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public OHashIndexBucket(final OCacheEntry cacheEntry) {
    super(cacheEntry);
  }

  public final void init(final int depth) {
    setByteValue(DEPTH_OFFSET, (byte) depth);
    setIntValue(FREE_POINTER_OFFSET, MAX_BUCKET_SIZE_BYTES);
    setIntValue(SIZE_OFFSET, 0);
  }

  public final Entry<K, V> find(final K key, final long hashCode, final OBinarySerializer<K> keySerializer,
      final OBinarySerializer<V> valueSerializer, final OEncryption encryption) {
    final int index = binarySearch(key, hashCode, keySerializer, encryption);
    if (index < 0)
      return null;

    return getEntry(index, encryption, keySerializer, valueSerializer);
  }

  int binarySearch(final K key, final long hashCode, final OBinarySerializer<K> keySerializer, final OEncryption encryption) {
    int low = 0;
    int high = size() - 1;

    while (low <= high) {
      final int mid = (low + high) >>> 1;

      final long midHashCode = getHashCode(mid);
      final int cmp;
      if (lessThanUnsigned(midHashCode, hashCode))
        cmp = -1;
      else if (greaterThanUnsigned(midHashCode, hashCode))
        cmp = 1;
      else {
        final K midVal = getKey(mid, keySerializer, encryption);
        //noinspection unchecked
        cmp = keyComparator.compare(midVal, key);
      }

      if (cmp < 0)
        low = mid + 1;
      else if (cmp > 0)
        high = mid - 1;
      else
        return mid; // key found
    }
    return -(low + 1); // key not found.
  }

  private static boolean lessThanUnsigned(final long longOne, final long longTwo) {
    return (longOne + Long.MIN_VALUE) < (longTwo + Long.MIN_VALUE);
  }

  private static boolean greaterThanUnsigned(final long longOne, final long longTwo) {
    return (longOne + Long.MIN_VALUE) > (longTwo + Long.MIN_VALUE);
  }

  public final Entry<K, V> getEntry(final int index, final OEncryption encryption, final OBinarySerializer<K> keySerializer,
      final OBinarySerializer<V> valueSerializer) {
    int entryPosition = getIntValue(POSITIONS_ARRAY_OFFSET + index * OIntegerSerializer.INT_SIZE);

    final long hashCode = getLongValue(entryPosition);
    entryPosition += OLongSerializer.LONG_SIZE;

    final K key;
    if (encryption == null) {
      key = deserializeFromDirectMemory(keySerializer, entryPosition);

      entryPosition += getObjectSizeInDirectMemory(keySerializer, entryPosition);
    } else {
      final int encryptedLength = getIntValue(entryPosition);
      entryPosition += OIntegerSerializer.INT_SIZE;

      final byte[] encryptedKey = getBinaryValue(entryPosition, encryptedLength);
      entryPosition += encryptedLength;

      final byte[] binaryKey = encryption.decrypt(encryptedKey);

      key = keySerializer.deserializeNativeObject(binaryKey, 0);
    }

    final V value = deserializeFromDirectMemory(valueSerializer, entryPosition);
    return new Entry<>(key, value, hashCode);
  }

  private RawEntry getRawEntry(final int index, final OEncryption encryption, final OBinarySerializer<K> keySerializer,
      final OBinarySerializer<V> valueSerializer) {
    int entryPosition = getIntValue(POSITIONS_ARRAY_OFFSET + index * OIntegerSerializer.INT_SIZE);
    final int startPosition = entryPosition;

    final long hashCode = getLongValue(entryPosition);
    entryPosition += OLongSerializer.LONG_SIZE;

    final int keySize;
    if (encryption == null) {
      keySize = getObjectSizeInDirectMemory(keySerializer, entryPosition);
      entryPosition += keySize;
    } else {
      keySize = getIntValue(entryPosition);
      entryPosition += OIntegerSerializer.INT_SIZE + keySize;
    }

    final int valueSize = getObjectSizeInDirectMemory(valueSerializer, entryPosition);
    return new RawEntry(getBinaryValue(startPosition, keySize + valueSize + OLongSerializer.LONG_SIZE), hashCode, keySize,
        valueSize);
  }

  final byte[] getRawValue(final int index, final boolean isEncrypted, final OBinarySerializer<K> keySerializer,
      final OBinarySerializer<V> valueSerializer) {
    int entryPosition = getIntValue(POSITIONS_ARRAY_OFFSET + index * OIntegerSerializer.INT_SIZE);

    // skip hash code
    entryPosition += OLongSerializer.LONG_SIZE;

    if (!isEncrypted) {
      // skip key
      entryPosition += getObjectSizeInDirectMemory(keySerializer, entryPosition);
    } else {
      final int encryptedLength = getIntValue(entryPosition);
      entryPosition += encryptedLength + OIntegerSerializer.INT_SIZE;
    }

    final int valueSize = getObjectSizeInDirectMemory(valueSerializer, entryPosition);
    return getBinaryValue(entryPosition, valueSize);
  }

  /**
   * Obtains the value stored under the given index in this bucket.
   *
   * @param index the value index.
   *
   * @return the obtained value.
   */
  public final V getValue(final int index, final boolean isEncrypted, final OBinarySerializer<K> keySerializer,
      final OBinarySerializer<V> valueSerializer) {
    int entryPosition = getIntValue(POSITIONS_ARRAY_OFFSET + index * OIntegerSerializer.INT_SIZE);

    // skip hash code
    entryPosition += OLongSerializer.LONG_SIZE;

    if (!isEncrypted) {
      // skip key
      entryPosition += getObjectSizeInDirectMemory(keySerializer, entryPosition);
    } else {
      final int encryptedLength = getIntValue(entryPosition);
      entryPosition += encryptedLength + OIntegerSerializer.INT_SIZE;
    }

    return deserializeFromDirectMemory(valueSerializer, entryPosition);
  }

  private long getHashCode(final int index) {
    final int entryPosition = getIntValue(POSITIONS_ARRAY_OFFSET + index * OIntegerSerializer.INT_SIZE);
    return getLongValue(entryPosition);
  }

  public final K getKey(final int index, final OBinarySerializer<K> keySerializer, final OEncryption encryption) {
    final int entryPosition = getIntValue(POSITIONS_ARRAY_OFFSET + index * OIntegerSerializer.INT_SIZE);

    if (encryption == null) {
      return deserializeFromDirectMemory(keySerializer, entryPosition + OLongSerializer.LONG_SIZE);
    } else {
      final int encryptedLength = getIntValue(entryPosition + OLongSerializer.LONG_SIZE);
      final byte[] encryptedBinaryKey = getBinaryValue(entryPosition + OLongSerializer.LONG_SIZE + OIntegerSerializer.INT_SIZE,
          encryptedLength);
      final byte[] decryptedBinaryKey = encryption.decrypt(encryptedBinaryKey);
      return keySerializer.deserializeNativeObject(decryptedBinaryKey, 0);
    }
  }

  public final int getIndex(final long hashCode, final K key, final OBinarySerializer<K> keySerializer,
      final OEncryption encryption) {
    return binarySearch(key, hashCode, keySerializer, encryption);
  }

  public final int size() {
    return getIntValue(SIZE_OFFSET);
  }

  public final Iterator<Entry<K, V>> iterator(final OBinarySerializer<K> keySerializer, final OBinarySerializer<V> valueSerializer,
      final OEncryption encryption) {
    return new EntryIterator(0, encryption, keySerializer, valueSerializer);
  }

  public final Iterator<RawEntry> rawIterator(final OBinarySerializer<K> keySerializer, final OBinarySerializer<V> valueSerializer,
      final OEncryption encryption) {
    return new RawEntryIterator(encryption, keySerializer, valueSerializer);
  }

  public final Iterator<Entry<K, V>> iterator(final int index, final OBinarySerializer<K> keySerializer,
      final OBinarySerializer<V> valueSerializer, final OEncryption encryption) {
    return new EntryIterator(index, encryption, keySerializer, valueSerializer);
  }

  public int getContentSize() {
    return POSITIONS_ARRAY_OFFSET + size() * OIntegerSerializer.INT_SIZE + (MAX_BUCKET_SIZE_BYTES - getIntValue(
        FREE_POINTER_OFFSET));
  }

  public final void updateEntry(final int index, final byte[] value, final int oldValueSize, final int keySize) {
    int entryPosition = getIntValue(POSITIONS_ARRAY_OFFSET + index * OIntegerSerializer.INT_SIZE);
    entryPosition += OLongSerializer.LONG_SIZE + keySize;

    final byte[] oldValue = getBinaryValue(entryPosition, oldValueSize);
    setBinaryValue(entryPosition, value);

    addPageOperation(new OHashIndexBucketUpdateEntryPageOperation(index, keySize, value.length, oldValue));
  }

  public final void deleteEntry(final int index, final int keySize, final int valueSize) {
    final int freePointer = getIntValue(FREE_POINTER_OFFSET);

    final int positionOffset = POSITIONS_ARRAY_OFFSET + index * OIntegerSerializer.INT_SIZE;
    final int entryPosition = getIntValue(positionOffset);

    final long hashCode = getLongValue(entryPosition);
    final byte[] key = getBinaryValue(entryPosition + OLongSerializer.LONG_SIZE, keySize);
    final byte[] value = getBinaryValue(entryPosition + OLongSerializer.LONG_SIZE + keySize, valueSize);

    final int entrySize = keySize + valueSize + OLongSerializer.LONG_SIZE;

    moveData(positionOffset + OIntegerSerializer.INT_SIZE, positionOffset,
        size() * OIntegerSerializer.INT_SIZE - (index + 1) * OIntegerSerializer.INT_SIZE);

    if (entryPosition > freePointer)
      moveData(freePointer, freePointer + entrySize, entryPosition - freePointer);

    int currentPositionOffset = POSITIONS_ARRAY_OFFSET;
    final int size = size();
    for (int i = 0; i < size - 1; i++) {
      final int currentEntryPosition = getIntValue(currentPositionOffset);
      if (currentEntryPosition < entryPosition)
        setIntValue(currentPositionOffset, currentEntryPosition + entrySize);
      currentPositionOffset += OIntegerSerializer.INT_SIZE;
    }

    setIntValue(FREE_POINTER_OFFSET, freePointer + entrySize);
    setIntValue(SIZE_OFFSET, size - 1);

    addPageOperation(new OHashIndexBucketDeleteEntryPageOperation(index, hashCode, key, value));
  }

  public final boolean addEntry(final int index, final long hashCode, final byte[] key, final byte[] value) {
    final int entreeSize = key.length + value.length + OLongSerializer.LONG_SIZE;
    final int freePointer = getIntValue(FREE_POINTER_OFFSET);

    final int size = size();
    if (freePointer - entreeSize < POSITIONS_ARRAY_OFFSET + (size + 1) * OIntegerSerializer.INT_SIZE) {
      return false;
    }

    insertEntry(hashCode, key, value, index, entreeSize);

    addPageOperation(new OHashIndexBucketAddEntryPageOperation(index, key.length, value.length));
    return true;
  }

  private void insertEntry(final long hashCode, final byte[] key, final byte[] value, final int insertionPoint,
      final int entreeSize) {
    final int freePointer = getIntValue(FREE_POINTER_OFFSET);
    final int size = size();

    final int positionsOffset = insertionPoint * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET;

    moveData(positionsOffset, positionsOffset + OIntegerSerializer.INT_SIZE,
        size() * OIntegerSerializer.INT_SIZE - insertionPoint * OIntegerSerializer.INT_SIZE);

    final int entreePosition = freePointer - entreeSize;
    setIntValue(positionsOffset, entreePosition);
    serializeEntry(hashCode, key, value, entreePosition);

    setIntValue(FREE_POINTER_OFFSET, entreePosition);
    setIntValue(SIZE_OFFSET, size + 1);
  }

  final void appendEntry(final byte[] entry, final int keySize, final int valueSize) {
    final int index = size();
    final int positionsOffset = index * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET;
    final int entreeSize = entry.length;

    final int freePointer = getIntValue(FREE_POINTER_OFFSET);
    final int entreePosition = freePointer - entreeSize;

    setIntValue(positionsOffset, entreePosition);
    setBinaryValue(entreePosition, entry);

    setIntValue(FREE_POINTER_OFFSET, freePointer - entreeSize);
    setIntValue(SIZE_OFFSET, size() + 1);

    addPageOperation(new OHashIndexBucketAddEntryPageOperation(index, keySize, valueSize));
  }

  private void serializeEntry(final long hashCode, final byte[] key, final byte[] value, int entryOffset) {
    setLongValue(entryOffset, hashCode);
    entryOffset += OLongSerializer.LONG_SIZE;

    entryOffset += setBinaryValue(entryOffset, key);
    setBinaryValue(entryOffset, value);
  }

  public final int getDepth() {
    return getByteValue(DEPTH_OFFSET);
  }

  public void setDepth(final int depth) {
    final byte oldDepth = getByteValue(DEPTH_OFFSET);

    setByteValue(DEPTH_OFFSET, (byte) depth);
    addPageOperation(new OHashIndexBucketSetDepthPageOperation(oldDepth));
  }

  public static final class Entry<K, V> {
    public final K    key;
    public final V    value;
    final        long hashCode;

    Entry(final K key, final V value, final long hashCode) {
      this.key = key;
      this.value = value;
      this.hashCode = hashCode;
    }
  }

  private final class EntryIterator implements Iterator<Entry<K, V>> {
    private       int                  currentIndex;
    private final OEncryption          encryption;
    private final OBinarySerializer<K> keySerializer;
    private final OBinarySerializer<V> valueSerializer;

    private EntryIterator(final int currentIndex, final OEncryption encryption, final OBinarySerializer<K> keySerializer,
        final OBinarySerializer<V> valueSerializer) {
      this.currentIndex = currentIndex;
      this.encryption = encryption;
      this.keySerializer = keySerializer;
      this.valueSerializer = valueSerializer;
    }

    @Override
    public boolean hasNext() {
      return currentIndex < size();
    }

    @Override
    public Entry<K, V> next() {
      if (currentIndex >= size())
        throw new NoSuchElementException("Iterator was reached last element");

      final Entry<K, V> entry = getEntry(currentIndex, encryption, keySerializer, valueSerializer);
      currentIndex++;
      return entry;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("Remove operation is not supported");
    }
  }

  static final class RawEntry {
    final byte[] entry;
    final long   hashCode;
    final int    keySize;
    final int    valueSize;

    RawEntry(final byte[] entry, final long hashCode, final int keySize, final int valueSize) {
      this.entry = entry;
      this.hashCode = hashCode;
      this.keySize = keySize;
      this.valueSize = valueSize;
    }
  }

  private final class RawEntryIterator implements Iterator<RawEntry> {
    private       int                  currentIndex;
    private final OEncryption          encryption;
    private final OBinarySerializer<K> keySerializer;
    private final OBinarySerializer<V> valueSerializer;

    private RawEntryIterator(final OEncryption encryption, final OBinarySerializer<K> keySerializer,
        final OBinarySerializer<V> valueSerializer) {
      this.currentIndex = 0;
      this.encryption = encryption;
      this.keySerializer = keySerializer;
      this.valueSerializer = valueSerializer;
    }

    @Override
    public boolean hasNext() {
      return currentIndex < size();
    }

    @Override
    public RawEntry next() {
      if (currentIndex >= size()) {
        throw new NoSuchElementException("Iterator was reached last element");
      }

      final RawEntry entry = getRawEntry(currentIndex, encryption, keySerializer, valueSerializer);
      currentIndex++;
      return entry;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("Remove operation is not supported");
    }
  }
}
