
package com.github.jni.timbersawjni;

//Junit 4 Packages
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.junit.Assert.*;

class TimberSawJNITest {

    private TimberSawJNI db;
    private File dbDir;

    @Before
    void setUp() {
        dbDir = new File("testDb");
        db = new TimberSawJNI(dbDir, null);
        assertNotNull(db);
    }

    @After
    void tearDown() {
        if (db != null) {
            db.close();
            assertTrue(dbDir.exists());
            String[] files = dbDir.list();
            if (files != null) {
                for (String fileName : files) {
                    assertTrue(new File(dbDir, fileName).delete());
                }
            }
            assertTrue(dbDir.delete());
        }
    }

    @Test
    void openWithBloomFilter() {
        db.close();
        TimberSawJNI.Options options = new TimberSawJNI.Options();
        options.bloomFilterBitsPerKey = 10;
        db = new TimberSawJNI(dbDir, options);
    }

    @Test
    void openWithCache() {
        db.close();
        TimberSawJNI.Options options = new TimberSawJNI.Options();
        options.cacheSizeBytes = 10_000_000;
        db = new TimberSawJNI(dbDir, options);
    }

    @Test
    void openWithNoCompression() {
        db.close();
        TimberSawJNI.Options options = new TimberSawJNI.Options();
        options.compression = false;
        db = new TimberSawJNI(dbDir, options);
    }

    @Test
    void iterator_iterateForward() {
        db.put(b("a"), b("av"));
        db.put(b("b"), b("bv"));
        db.put(b("c"), b("cv"));
        StringBuilder sb = new StringBuilder();
        try (TimberSawJNI.Iterator it = db.iterator()) {
            for (it.seekToFirst(); it.isValid(); it.next()) {
                sb.append(new String(it.key(), StandardCharsets.UTF_8));
                sb.append('=');
                sb.append(new String(it.value(), StandardCharsets.UTF_8));
                sb.append(';');
            }
        }
        assertEquals("a=av;b=bv;c=cv;", sb.toString());
    }

    @Test
    void iterator_noDefault() {
        db.put(b("a"), b("av"));
        db.put(b("b"), b("bv"));
        db.put(b("c"), b("cv"));
        StringBuilder sb = new StringBuilder();
        try (TimberSawJNI.Iterator it = db.iterator()) {
            for (; it.isValid(); it.next()) {
                sb.append(new String(it.key(), StandardCharsets.UTF_8));
                sb.append('=');
                sb.append(new String(it.value(), StandardCharsets.UTF_8));
                sb.append(';');
            }
        }
        assertEquals("", sb.toString());
    }

    @Test
    void iterator_iterateBackward() {
        db.put(b("a"), b("av"));
        db.put(b("b"), b("bv"));
        db.put(b("c"), b("cv"));
        StringBuilder sb = new StringBuilder();
        try (TimberSawJNI.Iterator it = db.iterator()) {
            for (it.seekToLast(); it.isValid(); it.prev()) {
                sb.append(new String(it.key(), StandardCharsets.UTF_8));
                sb.append('=');
                sb.append(new String(it.value(), StandardCharsets.UTF_8));
                sb.append(';');
            }
        }
        assertEquals("c=cv;b=bv;a=av;", sb.toString());
    }

    @Test
    void iterator_iterateFromMid() {
        db.put(b("a"), b("av"));
        db.put(b("b"), b("bv"));
        db.put(b("c"), b("cv"));
        StringBuilder sb = new StringBuilder();
        try (TimberSawJNI.Iterator it = db.iterator()) {
            for (it.seek(b("b")); it.isValid(); it.next()) {
                sb.append(new String(it.key(), StandardCharsets.UTF_8));
                sb.append('=');
                sb.append(new String(it.value(), StandardCharsets.UTF_8));
                sb.append(';');
            }
        }
        assertEquals("b=bv;c=cv;", sb.toString());
    }

    @Test
    void createWriteBatch() {
        byte[] key1 = b("key1");
        byte[] key2 = b("key2");
        byte[] key3 = b("key3");
        db.put(key1, b("a"));
        db.put(key2, b("b"));
        try (TimberSawJNI.WriteBatch batch = db.createWriteBatch()) {
            batch.put(key1, b("aaa"));
            batch.delete(key2);
            batch.put(key3, b("ccc"));
        }
        assertArrayEquals(b("aaa"), db.get(key1));
        assertNull(db.get(key2));
        assertArrayEquals(b("ccc"), db.get(key3));
    }

    @Test
    void createWriteBatch_distinctWrite() {
        byte[] key1 = b("key1");
        byte[] key2 = b("key2");
        byte[] key3 = b("key3");
        db.put(key1, b("a"));
        db.put(key2, b("b"));
        TimberSawJNI.WriteBatch batch = db.createWriteBatch();
        batch.put(key1, b("aaa"));
        batch.delete(key2);
        batch.put(key3, b("ccc"));
        assertTrue(batch.write());
        assertFalse(batch.write());
        assertArrayEquals(b("aaa"), db.get(key1));
        assertNull(db.get(key2));
        assertArrayEquals(b("ccc"), db.get(key3));
    }

    @Test
    void createWriteBatch_distinctWriteAndClose() {
        byte[] key1 = b("key1");
        byte[] key2 = b("key2");
        byte[] key3 = b("key3");
        db.put(key1, b("a"));
        db.put(key2, b("b"));
        TimberSawJNI.WriteBatch batch = db.createWriteBatch();
        batch.put(key1, b("aaa"));
        batch.delete(key2);
        batch.put(key3, b("ccc"));
        assertTrue(batch.write());
        batch.close();
        assertArrayEquals(b("aaa"), db.get(key1));
        assertNull(db.get(key2));
        assertArrayEquals(b("ccc"), db.get(key3));
    }

    @Test
    void delete() {
        byte[] key = b("key");
        db.put(key, b("value"));
        assertNotNull(db.get(key));
        assertTrue(db.delete(key));
        assertNull(db.get(key));
    }

    @Test
    void delete_nonExistent() {
        byte[] key = b("key");
        assertTrue(db.delete(key));
    }

    @Test
    void get_nonExistent() {
        assertNull(db.get(new byte[0]));
    }

    @Test
    void getAndPut_empty() {
        db.put(new byte[0], new byte[0]);
        assertArrayEquals(new byte[0], db.get(new byte[0]));
    }

    @Test
    void getAndPut_notEmptyValue() {
        db.put(new byte[0], new byte[1]);
        assertArrayEquals(new byte[1], db.get(new byte[0]));
    }

    @Test
    void getAndPut_largeValue() {
        byte[] key = b("efweio efuhdioef  epf oieifoj");
        byte[] value = new byte[10000000];
        Arrays.fill(value, (byte) 0xff);
        assertTrue(db.put(key, value));
        assertArrayEquals(value, db.get(key));
    }

    @Test
    void put_update() {
        assertTrue(db.put(b("a"), b("1")));
        assertTrue(db.put(b("a"), b("2")));
        assertArrayEquals(b("2"), db.get(b("a")));
    }

    @Test
    void close() {
        db.close();
        db.close();
    }

    private static byte[] b(String s) {
        return s == null ? null : s.getBytes(StandardCharsets.UTF_8);
    }
}
