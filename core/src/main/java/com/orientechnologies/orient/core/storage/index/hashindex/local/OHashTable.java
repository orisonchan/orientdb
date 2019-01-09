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
import com.orientechnologies.orient.core.encryption.OEncryption;
import com.orientechnologies.orient.core.index.OIndexEngine;
import com.orientechnologies.orient.core.metadata.schema.OType;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.util.Comparator;

public interface OHashTable<K, V> {
  void create(OBinarySerializer<K> keySerializer, OBinarySerializer<V> valueSerializer, OType[] keyTypes, OEncryption encryption,
      OHashFunction<K> keyHashFunction, boolean nullKeyIsSupported) throws IOException;

  OBinarySerializer<K> getKeySerializer();

  void setKeySerializer(OBinarySerializer<K> keySerializer) throws IOException;

  OBinarySerializer<V> getValueSerializer();

  void setValueSerializer(OBinarySerializer<V> valueSerializer) throws IOException;

  V get(K key);

  /**
   * Puts the given value under the given key into this hash table. Validates the operation using the provided validator.
   *
   * @param key       the key to put the value under.
   * @param value     the value to put.
   * @param validator the operation validator.
   *
   * @return {@code true} if the validator allowed the put, {@code false} otherwise.
   *
   * @see OIndexEngine.Validator#validate(Object, Object, Object)
   */
  boolean validatedPut(K key, V value, OIndexEngine.Validator<K, V> validator) throws IOException;

  void put(K key, V value) throws IOException;

  V remove(K key) throws IOException;

  void clear() throws IOException;

  OHashIndexBucket.Entry<K, V>[] higherEntries(K key);

  OHashIndexBucket.Entry<K, V>[] higherEntries(K key, int limit);

  void load(String name, OType[] keyTypes, boolean nullKeyIsSupported, OEncryption encryption, OHashFunction<K> keyHashFunction);

  void deleteWithoutLoad(String name) throws IOException;

  OHashIndexBucket.Entry<K, V>[] ceilingEntries(K key);

  OHashIndexBucket.Entry<K, V> firstEntry();

  OHashIndexBucket.Entry<K, V> lastEntry();

  OHashIndexBucket.Entry<K, V>[] lowerEntries(K key);

  OHashIndexBucket.Entry<K, V>[] floorEntries(K key);

  long size();

  void close();

  void delete() throws IOException;

  void flush();

  boolean isNullKeyIsSupported();

  /**
   * Acquires exclusive lock in the active atomic operation running on the current thread for this hash table.
   */
  void acquireAtomicExclusiveLock();

  String getName();

  final class BucketPath {
    final BucketPath parent;
    final int        hashMapOffset;
    final int        itemIndex;
    final int        nodeIndex;
    final int        nodeGlobalDepth;
    final int        nodeLocalDepth;

    BucketPath(final BucketPath parent, final int hashMapOffset, final int itemIndex, final int nodeIndex, final int nodeLocalDepth,
        final int nodeGlobalDepth) {
      this.parent = parent;
      this.hashMapOffset = hashMapOffset;
      this.itemIndex = itemIndex;
      this.nodeIndex = nodeIndex;
      this.nodeGlobalDepth = nodeGlobalDepth;
      this.nodeLocalDepth = nodeLocalDepth;
    }
  }

  final class BucketSplitResult {
    final long updatedBucketPointer;
    final long newBucketPointer;
    final int  newDepth;

    BucketSplitResult(final long updatedBucketPointer, final long newBucketPointer, final int newDepth) {
      this.updatedBucketPointer = updatedBucketPointer;
      this.newBucketPointer = newBucketPointer;
      this.newDepth = newDepth;
    }
  }

  final class NodeSplitResult {
    final long[]  newNode;
    final boolean allLeftHashMapsEqual;
    final boolean allRightHashMapsEqual;

    @SuppressFBWarnings("EI_EXPOSE_REP2")
    NodeSplitResult(final long[] newNode, final boolean allLeftHashMapsEqual, final boolean allRightHashMapsEqual) {
      this.newNode = newNode;
      this.allLeftHashMapsEqual = allLeftHashMapsEqual;
      this.allRightHashMapsEqual = allRightHashMapsEqual;
    }
  }

  @SuppressFBWarnings(value = "SE_COMPARATOR_SHOULD_BE_SERIALIZABLE")
  final class KeyHashCodeComparator<K> implements Comparator<K> {
    private final Comparator<? super K> comparator = ODefaultComparator.INSTANCE;

    private final OHashFunction<K> keyHashFunction;

    KeyHashCodeComparator(final OHashFunction<K> keyHashFunction) {
      this.keyHashFunction = keyHashFunction;
    }

    @Override
    public int compare(final K keyOne, final K keyTwo) {
      final long hashCodeOne = keyHashFunction.hashCode(keyOne);
      final long hashCodeTwo = keyHashFunction.hashCode(keyTwo);

      if (greaterThanUnsigned(hashCodeOne, hashCodeTwo))
        return 1;
      if (lessThanUnsigned(hashCodeOne, hashCodeTwo))
        return -1;

      return comparator.compare(keyOne, keyTwo);
    }

    private static boolean lessThanUnsigned(final long longOne, final long longTwo) {
      return (longOne + Long.MIN_VALUE) < (longTwo + Long.MIN_VALUE);
    }

    private static boolean greaterThanUnsigned(final long longOne, final long longTwo) {
      return (longOne + Long.MIN_VALUE) > (longTwo + Long.MIN_VALUE);
    }
  }
}
