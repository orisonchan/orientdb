package com.orientechnologies.orient.core.storage.cache.local.wtinylfu.eviction;

import com.orientechnologies.common.directmemory.OByteBufferPool;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.OCacheEntryImpl;
import com.orientechnologies.orient.core.storage.cache.OCachePointer;
import com.orientechnologies.orient.core.storage.cache.local.wtinylfu.PageKey;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.concurrent.ConcurrentHashMap;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class WTinyLFUTest {

  @Test
  public void testEden() {
    ConcurrentHashMap<PageKey, OCacheEntry> data = new ConcurrentHashMap<>();
    @SuppressWarnings("unchecked")
    Admittor<PageKey> admittor = mock(Admittor.class);

    WTinyLFU wTinyLFU = new WTinyLFU(data, admittor);
    wTinyLFU.setMaxSize(15);

    OByteBufferPool pool = new OByteBufferPool(4 * 1024);
    OCachePointer cachePointer = new OCachePointer(pool.acquireDirect(true), pool, 1, 1);
    cachePointer.incrementReferrer();

    OCacheEntry one = new OCacheEntryImpl(1, 1, cachePointer, false);
    OCacheEntry two = new OCacheEntryImpl(1, 2, cachePointer, false);
    OCacheEntry three = new OCacheEntryImpl(1, 3, cachePointer, false);

    wTinyLFU.onAdd(one);
    wTinyLFU.onAdd(two);
    wTinyLFU.onAdd(three);

    Assert.assertEquals(3, wTinyLFU.getSize());
    Assert.assertEquals(15, wTinyLFU.getMaxSize());

    Assert.assertEquals(0, wTinyLFU.probation().length);
    Assert.assertEquals(0, wTinyLFU.protection().length);
    Assert.assertArrayEquals(new OCacheEntry[] { one, two, three }, wTinyLFU.eden());

    wTinyLFU.onAccess(two);

    Assert.assertEquals(0, wTinyLFU.probation().length);
    Assert.assertEquals(0, wTinyLFU.protection().length);
    Assert.assertArrayEquals(new OCacheEntry[] { one, three, two }, wTinyLFU.eden());

    wTinyLFU.onAccess(two);

    Assert.assertEquals(0, wTinyLFU.probation().length);
    Assert.assertEquals(0, wTinyLFU.protection().length);
    Assert.assertArrayEquals(new OCacheEntry[] { one, three, two }, wTinyLFU.eden());

    wTinyLFU.onAccess(one);

    Assert.assertEquals(0, wTinyLFU.probation().length);
    Assert.assertEquals(0, wTinyLFU.protection().length);
    Assert.assertArrayEquals(new OCacheEntry[] { three, two, one }, wTinyLFU.eden());

    Assert.assertEquals(3, wTinyLFU.getSize());

    cachePointer.decrementReferrer();
    Mockito.reset(admittor);
  }

  @Test
  public void testGoLastToProbation() {
    ConcurrentHashMap<PageKey, OCacheEntry> data = new ConcurrentHashMap<>();
    @SuppressWarnings("unchecked")
    Admittor<PageKey> admittor = mock(Admittor.class);

    WTinyLFU wTinyLFU = new WTinyLFU(data, admittor);
    wTinyLFU.setMaxSize(15);

    OByteBufferPool pool = new OByteBufferPool(4 * 1024);
    OCachePointer cachePointer = new OCachePointer(pool.acquireDirect(true), pool, 1, 1);
    cachePointer.incrementReferrer();

    OCacheEntry one = new OCacheEntryImpl(1, 1, cachePointer, false);
    OCacheEntry two = new OCacheEntryImpl(1, 2, cachePointer, false);
    OCacheEntry three = new OCacheEntryImpl(1, 3, cachePointer, false);
    OCacheEntry four = new OCacheEntryImpl(1, 4, cachePointer, false);

    wTinyLFU.onAdd(one);
    wTinyLFU.onAdd(two);
    wTinyLFU.onAdd(three);
    wTinyLFU.onAdd(four);

    Assert.assertArrayEquals(new OCacheEntry[] { one }, wTinyLFU.probation());
    Assert.assertEquals(0, wTinyLFU.protection().length);
    Assert.assertArrayEquals(new OCacheEntry[] { two, three, four }, wTinyLFU.eden());

    Assert.assertEquals(4, wTinyLFU.getSize());


    cachePointer.decrementReferrer();
    Mockito.reset(admittor);
  }

  @Test
  public void testRemoveLastProbation() {
    ConcurrentHashMap<PageKey, OCacheEntry> data = new ConcurrentHashMap<>();
    @SuppressWarnings("unchecked")
    Admittor<PageKey> admittor = mock(Admittor.class);

    WTinyLFU wTinyLFU = new WTinyLFU(data, admittor);
    wTinyLFU.setMaxSize(15);

    OByteBufferPool pool = new OByteBufferPool(4 * 1024);
    OCachePointer cachePointerOne = new OCachePointer(pool.acquireDirect(true), pool, 1, 1);
    cachePointerOne.incrementReadersReferrer();

    OCachePointer cachePointerTwo = new OCachePointer(pool.acquireDirect(true), pool, 1, 1);
    cachePointerTwo.incrementReadersReferrer();

    OCacheEntry one = new OCacheEntryImpl(1, 1, cachePointerOne, false);
    OCacheEntry two = new OCacheEntryImpl(1, 2, cachePointerTwo, false);
    OCacheEntry three = new OCacheEntryImpl(1, 3, cachePointerTwo, false);
    OCacheEntry four = new OCacheEntryImpl(1, 4, cachePointerTwo, false);
    OCacheEntry five = new OCacheEntryImpl(1, 5, cachePointerTwo, false);
    OCacheEntry six = new OCacheEntryImpl(1, 6, cachePointerTwo, false);

    when(admittor.frequency(new PageKey(1, 1))).thenReturn(1);
    when(admittor.frequency(new PageKey(1, 3))).thenReturn(1);

    wTinyLFU.onAdd(one);
    wTinyLFU.onAdd(two);
    wTinyLFU.onAdd(three);
    wTinyLFU.onAdd(four);
    wTinyLFU.onAdd(five);
    wTinyLFU.onAdd(six);

    Assert.assertArrayEquals(new OCacheEntry[] { two, three }, wTinyLFU.probation());
    Assert.assertEquals(0, wTinyLFU.protection().length);
    Assert.assertArrayEquals(new OCacheEntry[] { four, five, six }, wTinyLFU.eden());

    Assert.assertEquals(5, wTinyLFU.getSize());

    cachePointerTwo.decrementReferrer();
    Mockito.reset(admittor);
  }

  @Test
  public void testAccessProbationElement() {
    ConcurrentHashMap<PageKey, OCacheEntry> data = new ConcurrentHashMap<>();
    @SuppressWarnings("unchecked")
    Admittor<PageKey> admittor = mock(Admittor.class);

    WTinyLFU wTinyLFU = new WTinyLFU(data, admittor);
    wTinyLFU.setMaxSize(15);

    OByteBufferPool pool = new OByteBufferPool(4 * 1024);

    OCachePointer cachePointer = new OCachePointer(pool.acquireDirect(true), pool, 1, 1);
    cachePointer.incrementReadersReferrer();

    OCacheEntry one = new OCacheEntryImpl(1, 1, cachePointer, false);
    OCacheEntry two = new OCacheEntryImpl(1, 2, cachePointer, false);
    OCacheEntry three = new OCacheEntryImpl(1, 3, cachePointer, false);
    OCacheEntry four = new OCacheEntryImpl(1, 4, cachePointer, false);
    OCacheEntry five = new OCacheEntryImpl(1, 5, cachePointer, false);

    wTinyLFU.onAdd(one);
    wTinyLFU.onAdd(two);
    wTinyLFU.onAdd(three);
    wTinyLFU.onAdd(four);
    wTinyLFU.onAdd(five);

    Assert.assertArrayEquals(new OCacheEntry[] { one, two }, wTinyLFU.probation());
    Assert.assertEquals(0, wTinyLFU.protection().length);
    Assert.assertArrayEquals(new OCacheEntry[] { three, four, five }, wTinyLFU.eden());

    wTinyLFU.onAccess(one);

    Assert.assertArrayEquals(new OCacheEntry[] { two }, wTinyLFU.probation());
    Assert.assertArrayEquals(new OCacheEntry[] { one }, wTinyLFU.protection());
    Assert.assertArrayEquals(new OCacheEntry[] { three, four, five }, wTinyLFU.eden());

    Assert.assertEquals(5, wTinyLFU.getSize());


    cachePointer.decrementReferrer();
    Mockito.reset(admittor);
  }

  @Test
  public void testProbationProtectedInteraction() {
    ConcurrentHashMap<PageKey, OCacheEntry> data = new ConcurrentHashMap<>();
    @SuppressWarnings("unchecked")
    Admittor<PageKey> admittor = mock(Admittor.class);

    WTinyLFU wTinyLFU = new WTinyLFU(data, admittor);
    wTinyLFU.setMaxSize(15);

    OByteBufferPool pool = new OByteBufferPool(4 * 1024);

    OCachePointer cachePointer = new OCachePointer(pool.acquireDirect(true), pool, 1, 1);
    cachePointer.incrementReadersReferrer();

    OCacheEntry one = new OCacheEntryImpl(1, 1, cachePointer, false);
    OCacheEntry two = new OCacheEntryImpl(1, 2, cachePointer, false);
    OCacheEntry three = new OCacheEntryImpl(1, 3, cachePointer, false);
    OCacheEntry four = new OCacheEntryImpl(1, 4, cachePointer, false);
    OCacheEntry five = new OCacheEntryImpl(1, 5, cachePointer, false);
    OCacheEntry six = new OCacheEntryImpl(1, 6, cachePointer, false);
    OCacheEntry seven = new OCacheEntryImpl(1, 7, cachePointer, false);
    OCacheEntry eight = new OCacheEntryImpl(1, 8, cachePointer, false);
    OCacheEntry nine = new OCacheEntryImpl(1, 9, cachePointer, false);
    OCacheEntry ten = new OCacheEntryImpl(1, 10, cachePointer, false);
    OCacheEntry eleven = new OCacheEntryImpl(1, 11, cachePointer, false);
    OCacheEntry twelve = new OCacheEntryImpl(1, 12, cachePointer, false);
    OCacheEntry thirteen = new OCacheEntryImpl(1, 13, cachePointer, false);
    OCacheEntry fourteen = new OCacheEntryImpl(1, 14, cachePointer, false);
    OCacheEntry fifteen = new OCacheEntryImpl(1, 15, cachePointer, false);

    wTinyLFU.onAdd(one);
    wTinyLFU.onAdd(two);
    wTinyLFU.onAdd(three);
    wTinyLFU.onAdd(four);
    wTinyLFU.onAdd(five);

    wTinyLFU.onAccess(one);
    wTinyLFU.onAccess(two);

    wTinyLFU.onAdd(six);
    wTinyLFU.onAdd(seven);

    wTinyLFU.onAccess(three);
    wTinyLFU.onAccess(four);

    wTinyLFU.onAdd(eight);
    wTinyLFU.onAdd(nine);

    wTinyLFU.onAccess(five);
    wTinyLFU.onAccess(six);

    wTinyLFU.onAdd(ten);
    wTinyLFU.onAdd(eleven);

    wTinyLFU.onAccess(seven);
    wTinyLFU.onAccess(eight);

    wTinyLFU.onAdd(twelve);
    wTinyLFU.onAdd(thirteen);

    wTinyLFU.onAccess(nine);
    wTinyLFU.onAccess(ten);

    wTinyLFU.onAdd(fourteen);
    wTinyLFU.onAdd(fifteen);

    Assert.assertArrayEquals(new OCacheEntry[] { eleven, twelve }, wTinyLFU.probation());
    Assert
        .assertArrayEquals(new OCacheEntry[] { one, two, three, four, five, six, seven, eight, nine, ten }, wTinyLFU.protection());
    Assert.assertArrayEquals(new OCacheEntry[] { thirteen, fourteen, fifteen }, wTinyLFU.eden());

    wTinyLFU.onAccess(seven);

    Assert.assertArrayEquals(new OCacheEntry[] { eleven, twelve }, wTinyLFU.probation());
    Assert
        .assertArrayEquals(new OCacheEntry[] { one, two, three, four, five, six, eight, nine, ten, seven }, wTinyLFU.protection());
    Assert.assertArrayEquals(new OCacheEntry[] { thirteen, fourteen, fifteen }, wTinyLFU.eden());

    wTinyLFU.onAccess(eleven);

    Assert.assertArrayEquals(new OCacheEntry[] { twelve, one }, wTinyLFU.probation());
    Assert.assertArrayEquals(new OCacheEntry[] { two, three, four, five, six, eight, nine, ten, seven, eleven },
        wTinyLFU.protection());
    Assert.assertArrayEquals(new OCacheEntry[] { thirteen, fourteen, fifteen }, wTinyLFU.eden());

    Assert.assertEquals(15, wTinyLFU.getSize());


    cachePointer.decrementReferrer();
    Mockito.reset(admittor);
  }

  @Test
  public void testPromoteEdenToProbation() {
    ConcurrentHashMap<PageKey, OCacheEntry> data = new ConcurrentHashMap<>();
    @SuppressWarnings("unchecked")
    Admittor<PageKey> admittor = mock(Admittor.class);

    WTinyLFU wTinyLFU = new WTinyLFU(data, admittor);
    wTinyLFU.setMaxSize(15);

    OByteBufferPool pool = new OByteBufferPool(4 * 1024);

    OCachePointer cachePointerOne = new OCachePointer(pool.acquireDirect(true), pool, 1, 1);
    cachePointerOne.incrementReadersReferrer();

    OCachePointer cachePointerTwo = new OCachePointer(pool.acquireDirect(true), pool, 1, 1);
    cachePointerTwo.incrementReadersReferrer();

    OCacheEntry one = new OCacheEntryImpl(1, 1, cachePointerOne, false);
    OCacheEntry two = new OCacheEntryImpl(1, 2, cachePointerOne, false);
    OCacheEntry three = new OCacheEntryImpl(1, 3, cachePointerOne, false);
    OCacheEntry four = new OCacheEntryImpl(1, 4, cachePointerOne, false);
    OCacheEntry five = new OCacheEntryImpl(1, 5, cachePointerOne, false);
    OCacheEntry six = new OCacheEntryImpl(1, 6, cachePointerOne, false);
    OCacheEntry seven = new OCacheEntryImpl(1, 7, cachePointerOne, false);
    OCacheEntry eight = new OCacheEntryImpl(1, 8, cachePointerOne, false);
    OCacheEntry nine = new OCacheEntryImpl(1, 9, cachePointerOne, false);
    OCacheEntry ten = new OCacheEntryImpl(1, 10, cachePointerOne, false);
    OCacheEntry eleven = new OCacheEntryImpl(1, 11, cachePointerTwo, false);
    OCacheEntry twelve = new OCacheEntryImpl(1, 12, cachePointerOne, false);
    OCacheEntry thirteen = new OCacheEntryImpl(1, 13, cachePointerOne, false);
    OCacheEntry fourteen = new OCacheEntryImpl(1, 14, cachePointerOne, false);
    OCacheEntry fifteen = new OCacheEntryImpl(1, 15, cachePointerOne, false);
    OCacheEntry sixteen = new OCacheEntryImpl(1, 16, cachePointerOne, false);

    wTinyLFU.onAdd(one);
    wTinyLFU.onAdd(two);
    wTinyLFU.onAdd(three);
    wTinyLFU.onAdd(four);
    wTinyLFU.onAdd(five);

    wTinyLFU.onAccess(one);
    wTinyLFU.onAccess(two);

    wTinyLFU.onAdd(six);
    wTinyLFU.onAdd(seven);

    wTinyLFU.onAccess(three);
    wTinyLFU.onAccess(four);

    wTinyLFU.onAdd(eight);
    wTinyLFU.onAdd(nine);

    wTinyLFU.onAccess(five);
    wTinyLFU.onAccess(six);

    wTinyLFU.onAdd(ten);
    wTinyLFU.onAdd(eleven);

    wTinyLFU.onAccess(seven);
    wTinyLFU.onAccess(eight);

    wTinyLFU.onAdd(twelve);
    wTinyLFU.onAdd(thirteen);

    wTinyLFU.onAccess(nine);
    wTinyLFU.onAccess(ten);

    wTinyLFU.onAdd(fourteen);
    wTinyLFU.onAdd(fifteen);

    Assert.assertArrayEquals(new OCacheEntry[] { eleven, twelve }, wTinyLFU.probation());
    Assert
        .assertArrayEquals(new OCacheEntry[] { one, two, three, four, five, six, seven, eight, nine, ten }, wTinyLFU.protection());
    Assert.assertArrayEquals(new OCacheEntry[] { thirteen, fourteen, fifteen }, wTinyLFU.eden());

    Assert.assertEquals(15, wTinyLFU.getSize());

    when(admittor.frequency(new PageKey(1, 13))).thenReturn(1);
    when(admittor.frequency(new PageKey(1, 11))).thenReturn(1);

    wTinyLFU.onAdd(sixteen);

    Assert.assertArrayEquals(new OCacheEntry[] { twelve, thirteen }, wTinyLFU.probation());
    Assert
        .assertArrayEquals(new OCacheEntry[] { one, two, three, four, five, six, seven, eight, nine, ten }, wTinyLFU.protection());
    Assert.assertArrayEquals(new OCacheEntry[] { fourteen, fifteen, sixteen }, wTinyLFU.eden());

    Assert.assertEquals(15, wTinyLFU.getSize());

    cachePointerOne.decrementReferrer();
    Mockito.reset(admittor);
  }

  @Test
  public void testNotPromoteEdenToProbation() {
    ConcurrentHashMap<PageKey, OCacheEntry> data = new ConcurrentHashMap<>();
    @SuppressWarnings("unchecked")
    Admittor<PageKey> admittor = mock(Admittor.class);

    WTinyLFU wTinyLFU = new WTinyLFU(data, admittor);
    wTinyLFU.setMaxSize(15);

    OByteBufferPool pool = new OByteBufferPool(4 * 1024);

    OCachePointer cachePointerOne = new OCachePointer(pool.acquireDirect(true), pool, 1, 1);
    cachePointerOne.incrementReadersReferrer();

    OCachePointer cachePointerTwo = new OCachePointer(pool.acquireDirect(true), pool, 1, 1);
    cachePointerTwo.incrementReadersReferrer();

    OCacheEntry one = new OCacheEntryImpl(1, 1, cachePointerOne, false);
    OCacheEntry two = new OCacheEntryImpl(1, 2, cachePointerOne, false);
    OCacheEntry three = new OCacheEntryImpl(1, 3, cachePointerOne, false);
    OCacheEntry four = new OCacheEntryImpl(1, 4, cachePointerOne, false);
    OCacheEntry five = new OCacheEntryImpl(1, 5, cachePointerOne, false);
    OCacheEntry six = new OCacheEntryImpl(1, 6, cachePointerOne, false);
    OCacheEntry seven = new OCacheEntryImpl(1, 7, cachePointerOne, false);
    OCacheEntry eight = new OCacheEntryImpl(1, 8, cachePointerOne, false);
    OCacheEntry nine = new OCacheEntryImpl(1, 9, cachePointerOne, false);
    OCacheEntry ten = new OCacheEntryImpl(1, 10, cachePointerOne, false);
    OCacheEntry eleven = new OCacheEntryImpl(1, 11, cachePointerOne, false);
    OCacheEntry twelve = new OCacheEntryImpl(1, 12, cachePointerOne, false);
    OCacheEntry thirteen = new OCacheEntryImpl(1, 13, cachePointerTwo, false);
    OCacheEntry fourteen = new OCacheEntryImpl(1, 14, cachePointerOne, false);
    OCacheEntry fifteen = new OCacheEntryImpl(1, 15, cachePointerOne, false);
    OCacheEntry sixteen = new OCacheEntryImpl(1, 16, cachePointerOne, false);

    wTinyLFU.onAdd(one);
    wTinyLFU.onAdd(two);
    wTinyLFU.onAdd(three);
    wTinyLFU.onAdd(four);
    wTinyLFU.onAdd(five);

    wTinyLFU.onAccess(one);
    wTinyLFU.onAccess(two);

    wTinyLFU.onAdd(six);
    wTinyLFU.onAdd(seven);

    wTinyLFU.onAccess(three);
    wTinyLFU.onAccess(four);

    wTinyLFU.onAdd(eight);
    wTinyLFU.onAdd(nine);

    wTinyLFU.onAccess(five);
    wTinyLFU.onAccess(six);

    wTinyLFU.onAdd(ten);
    wTinyLFU.onAdd(eleven);

    wTinyLFU.onAccess(seven);
    wTinyLFU.onAccess(eight);

    wTinyLFU.onAdd(twelve);
    wTinyLFU.onAdd(thirteen);

    wTinyLFU.onAccess(nine);
    wTinyLFU.onAccess(ten);

    wTinyLFU.onAdd(fourteen);
    wTinyLFU.onAdd(fifteen);

    Assert.assertArrayEquals(new OCacheEntry[] { eleven, twelve }, wTinyLFU.probation());
    Assert
        .assertArrayEquals(new OCacheEntry[] { one, two, three, four, five, six, seven, eight, nine, ten }, wTinyLFU.protection());
    Assert.assertArrayEquals(new OCacheEntry[] { thirteen, fourteen, fifteen }, wTinyLFU.eden());

    Assert.assertEquals(15, wTinyLFU.getSize());

    when(admittor.frequency(new PageKey(1, 13))).thenReturn(1);
    when(admittor.frequency(new PageKey(1, 11))).thenReturn(2);

    wTinyLFU.onAdd(sixteen);

    Assert.assertArrayEquals(new OCacheEntry[] { eleven, twelve }, wTinyLFU.probation());
    Assert
        .assertArrayEquals(new OCacheEntry[] { one, two, three, four, five, six, seven, eight, nine, ten }, wTinyLFU.protection());
    Assert.assertArrayEquals(new OCacheEntry[] { fourteen, fifteen, sixteen }, wTinyLFU.eden());

    Assert.assertEquals(15, wTinyLFU.getSize());

    cachePointerOne.decrementReferrer();
    Mockito.reset(admittor);
  }

  @Test
  public void testFailFreezeVictim() {
    ConcurrentHashMap<PageKey, OCacheEntry> data = new ConcurrentHashMap<>();
    @SuppressWarnings("unchecked")
    Admittor<PageKey> admittor = mock(Admittor.class);

    WTinyLFU wTinyLFU = new WTinyLFU(data, admittor);
    wTinyLFU.setMaxSize(15);

    OByteBufferPool pool = new OByteBufferPool(4 * 1024);

    OCachePointer cachePointerOne = new OCachePointer(pool.acquireDirect(true), pool, 1, 1);
    cachePointerOne.incrementReadersReferrer();

    OCachePointer cachePointerTwo = new OCachePointer(pool.acquireDirect(true), pool, 1, 1);
    cachePointerTwo.incrementReadersReferrer();

    OCacheEntry one = new OCacheEntryImpl(1, 1, cachePointerOne, false);
    OCacheEntry two = new OCacheEntryImpl(1, 2, cachePointerOne, false);
    OCacheEntry three = new OCacheEntryImpl(1, 3, cachePointerOne, false);
    OCacheEntry four = new OCacheEntryImpl(1, 4, cachePointerOne, false);
    OCacheEntry five = new OCacheEntryImpl(1, 5, cachePointerOne, false);
    OCacheEntry six = new OCacheEntryImpl(1, 6, cachePointerOne, false);
    OCacheEntry seven = new OCacheEntryImpl(1, 7, cachePointerOne, false);
    OCacheEntry eight = new OCacheEntryImpl(1, 8, cachePointerOne, false);
    OCacheEntry nine = new OCacheEntryImpl(1, 9, cachePointerOne, false);
    OCacheEntry ten = new OCacheEntryImpl(1, 10, cachePointerOne, false);
    OCacheEntry eleven = new OCacheEntryImpl(1, 11, cachePointerOne, false);
    OCacheEntry twelve = new OCacheEntryImpl(1, 12, cachePointerTwo, false);
    OCacheEntry thirteen = new OCacheEntryImpl(1, 13, cachePointerOne, false);
    OCacheEntry fourteen = new OCacheEntryImpl(1, 14, cachePointerOne, false);
    OCacheEntry fifteen = new OCacheEntryImpl(1, 15, cachePointerOne, false);
    OCacheEntry sixteen = new OCacheEntryImpl(1, 16, cachePointerOne, false);

    wTinyLFU.onAdd(one);
    wTinyLFU.onAdd(two);
    wTinyLFU.onAdd(three);
    wTinyLFU.onAdd(four);
    wTinyLFU.onAdd(five);

    wTinyLFU.onAccess(one);
    wTinyLFU.onAccess(two);

    wTinyLFU.onAdd(six);
    wTinyLFU.onAdd(seven);

    wTinyLFU.onAccess(three);
    wTinyLFU.onAccess(four);

    wTinyLFU.onAdd(eight);
    wTinyLFU.onAdd(nine);

    wTinyLFU.onAccess(five);
    wTinyLFU.onAccess(six);

    wTinyLFU.onAdd(ten);
    wTinyLFU.onAdd(eleven);

    wTinyLFU.onAccess(seven);
    wTinyLFU.onAccess(eight);

    wTinyLFU.onAdd(twelve);
    wTinyLFU.onAdd(thirteen);

    wTinyLFU.onAccess(nine);
    wTinyLFU.onAccess(ten);

    wTinyLFU.onAdd(fourteen);
    wTinyLFU.onAdd(fifteen);

    Assert.assertArrayEquals(new OCacheEntry[] { eleven, twelve }, wTinyLFU.probation());
    Assert
        .assertArrayEquals(new OCacheEntry[] { one, two, three, four, five, six, seven, eight, nine, ten }, wTinyLFU.protection());
    Assert.assertArrayEquals(new OCacheEntry[] { thirteen, fourteen, fifteen }, wTinyLFU.eden());

    Assert.assertEquals(15, wTinyLFU.getSize());

    when(admittor.frequency(new PageKey(1, 11))).thenReturn(1);
    when(admittor.frequency(new PageKey(1, 12))).thenReturn(1);
    when(admittor.frequency(new PageKey(1, 13))).thenReturn(1);
    when(admittor.frequency(new PageKey(1, 14))).thenReturn(1);

    Assert.assertTrue(eleven.acquireEntry());

    wTinyLFU.onAdd(sixteen);

    Assert.assertArrayEquals(new OCacheEntry[] {thirteen, fourteen }, wTinyLFU.probation());
    Assert
        .assertArrayEquals(new OCacheEntry[] { one, two, three, four, five, six, seven, eight, nine, ten }, wTinyLFU.protection());
    Assert.assertArrayEquals(new OCacheEntry[] { fifteen, sixteen, eleven }, wTinyLFU.eden());

    Assert.assertEquals(15, wTinyLFU.getSize());

    cachePointerOne.decrementReferrer();
    Mockito.reset(admittor);
  }

  @Test
  public void testFailFreezeCandidate() {
    ConcurrentHashMap<PageKey, OCacheEntry> data = new ConcurrentHashMap<>();
    @SuppressWarnings("unchecked")
    Admittor<PageKey> admittor = mock(Admittor.class);

    WTinyLFU wTinyLFU = new WTinyLFU(data, admittor);
    wTinyLFU.setMaxSize(15);

    OByteBufferPool pool = new OByteBufferPool(4 * 1024);

    OCachePointer cachePointerOne = new OCachePointer(pool.acquireDirect(true), pool, 1, 1);
    cachePointerOne.incrementReadersReferrer();

    OCachePointer cachePointerTwo = new OCachePointer(pool.acquireDirect(true), pool, 1, 1);
    cachePointerTwo.incrementReadersReferrer();

    OCacheEntry one = new OCacheEntryImpl(1, 1, cachePointerOne, false);
    OCacheEntry two = new OCacheEntryImpl(1, 2, cachePointerOne, false);
    OCacheEntry three = new OCacheEntryImpl(1, 3, cachePointerOne, false);
    OCacheEntry four = new OCacheEntryImpl(1, 4, cachePointerOne, false);
    OCacheEntry five = new OCacheEntryImpl(1, 5, cachePointerOne, false);
    OCacheEntry six = new OCacheEntryImpl(1, 6, cachePointerOne, false);
    OCacheEntry seven = new OCacheEntryImpl(1, 7, cachePointerOne, false);
    OCacheEntry eight = new OCacheEntryImpl(1, 8, cachePointerOne, false);
    OCacheEntry nine = new OCacheEntryImpl(1, 9, cachePointerOne, false);
    OCacheEntry ten = new OCacheEntryImpl(1, 10, cachePointerOne, false);
    OCacheEntry eleven = new OCacheEntryImpl(1, 11, cachePointerOne, false);
    OCacheEntry twelve = new OCacheEntryImpl(1, 12, cachePointerOne, false);
    OCacheEntry thirteen = new OCacheEntryImpl(1, 13, cachePointerOne, false);
    OCacheEntry fourteen = new OCacheEntryImpl(1, 14, cachePointerTwo, false);
    OCacheEntry fifteen = new OCacheEntryImpl(1, 15, cachePointerOne, false);
    OCacheEntry sixteen = new OCacheEntryImpl(1, 16, cachePointerOne, false);

    wTinyLFU.onAdd(one);
    wTinyLFU.onAdd(two);
    wTinyLFU.onAdd(three);
    wTinyLFU.onAdd(four);
    wTinyLFU.onAdd(five);

    wTinyLFU.onAccess(one);
    wTinyLFU.onAccess(two);

    wTinyLFU.onAdd(six);
    wTinyLFU.onAdd(seven);

    wTinyLFU.onAccess(three);
    wTinyLFU.onAccess(four);

    wTinyLFU.onAdd(eight);
    wTinyLFU.onAdd(nine);

    wTinyLFU.onAccess(five);
    wTinyLFU.onAccess(six);

    wTinyLFU.onAdd(ten);
    wTinyLFU.onAdd(eleven);

    wTinyLFU.onAccess(seven);
    wTinyLFU.onAccess(eight);

    wTinyLFU.onAdd(twelve);
    wTinyLFU.onAdd(thirteen);

    wTinyLFU.onAccess(nine);
    wTinyLFU.onAccess(ten);

    wTinyLFU.onAdd(fourteen);
    wTinyLFU.onAdd(fifteen);

    Assert.assertArrayEquals(new OCacheEntry[] { eleven, twelve }, wTinyLFU.probation());
    Assert
        .assertArrayEquals(new OCacheEntry[] { one, two, three, four, five, six, seven, eight, nine, ten }, wTinyLFU.protection());
    Assert.assertArrayEquals(new OCacheEntry[] { thirteen, fourteen, fifteen }, wTinyLFU.eden());

    Assert.assertEquals(15, wTinyLFU.getSize());

    when(admittor.frequency(new PageKey(1, 14))).thenReturn(1);
    when(admittor.frequency(new PageKey(1, 13))).thenReturn(1);
    when(admittor.frequency(new PageKey(1, 11))).thenReturn(2);

    Assert.assertTrue(thirteen.acquireEntry());

    wTinyLFU.onAdd(sixteen);

    Assert.assertArrayEquals(new OCacheEntry[] { eleven, twelve }, wTinyLFU.probation());
    Assert
        .assertArrayEquals(new OCacheEntry[] { one, two, three, four, five, six, seven, eight, nine, ten }, wTinyLFU.protection());
    Assert.assertArrayEquals(new OCacheEntry[] { fifteen, sixteen, thirteen }, wTinyLFU.eden());

    Assert.assertEquals(15, wTinyLFU.getSize());

    cachePointerOne.decrementReferrer();
    Mockito.reset(admittor);
  }

}