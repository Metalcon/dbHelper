package de.metalcon.dbhelper;

import static org.fusesource.leveldbjni.JniDBFactory.asString;
import static org.fusesource.leveldbjni.JniDBFactory.factory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;

import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;

import de.metalcon.exceptions.MetalconException;
import de.metalcon.exceptions.MetalconRuntimeException;

/**
 * Central Class to be used to access levelDB storages. It must be initialized
 * via LevelDbHandler.initialize which may only be called once during runtime.
 * It creates only one storage and separates all entries created by different
 * Instance using a unique prefix for the keys.
 * 
 * @author Jonas Kunze (kunze.jonas@gmail.com)
 * 
 */
public class LevelDbHandler {

    private static DB db = null;

    /*
     * Path to the storage used by all instances
     */
    private static String DBPath_;

    /*
     * The prefixes of all running instances
     */
    private static HashSet<Long> keyPrefixes = new HashSet<Long>();

    /*
     * The prefix of this instance
     */
    private final byte[] keyPrefix;

    /**
     * Creates a levelDB DB object
     * 
     * @param DBPath
     *            Path to the directory where the levelDB should store its
     *            files. If it doesn't exist, the directory will be created.
     * @throws MetalconException
     */
    public static void initialize(final String DBPath) throws MetalconException {
        File f = new File(DBPath);

        if (!f.exists()) {
            if (!f.mkdirs()) {
                throw new MetalconException("Unable to create directory "
                        + DBPath);
            }
        }

        if (db == null) {
            try {
                Options options = new Options();
                options.createIfMissing(true);

                // options.logger(new Logger() {
                // public void log(String message) {
                // System.out.println(message);
                // }
                // });

                db = factory.open(new File(DBPath), options);
            } catch (IOException e) {
                throw new MetalconException("Unable to instanciate levelDB on "
                        + DBPath + ": " + e.getMessage());
            }
        } else {
            throw new MetalconException(
                    "LevelDBHandler has already been Initialized");
        }
        DBPath_ = DBPath;

        Runtime.getRuntime().addShutdownHook(new Thread() {

            @Override
            public void run() {
                try {
                    if (db != null) {
                        db.close();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    /**
     * 
     * @param keyPrefix
     *            Any long which is used to identify this instance.
     */
    public LevelDbHandler(
            final long keyPrefix) {
        synchronized (keyPrefixes) {
            if (!keyPrefixes.add(keyPrefix)) {
                throw new MetalconRuntimeException("Instanciated "
                        + LevelDbHandler.class.getName()
                        + " with a non-unique key prefix");
            }
        }

        this.keyPrefix = new byte[8];
        this.keyPrefix[0] = (byte) (keyPrefix >> 56);
        this.keyPrefix[1] = (byte) (keyPrefix >> 48);
        this.keyPrefix[2] = (byte) (keyPrefix >> 40);
        this.keyPrefix[3] = (byte) (keyPrefix >> 32);
        this.keyPrefix[4] = (byte) (keyPrefix >> 24);
        this.keyPrefix[5] = (byte) (keyPrefix >> 16);
        this.keyPrefix[6] = (byte) (keyPrefix >> 8);
        this.keyPrefix[7] = (byte) (keyPrefix);
    }

    /**
     * 
     * @param keyPrefix
     *            Any String which is used to identify this instance.
     */
    public LevelDbHandler(
            final String keyPrefix) {
        this(keyPrefix.hashCode() + 0xFFFFFFFFL/* 4 Byte */
                * keyPrefix.hashCode());
        if (db == null) {
            /*
             * Make sure that the developer does not forget to run initialize()
             */
            throw new MetalconRuntimeException("You have to call "
                    + LevelDbHandler.class.getName()
                    + ".initialize() before calling this constructor");
        }
    }

    /**
     * Completely deletes all data stored in the central levelDB (including all
     * data from all instances
     * 
     * @param areYouSure
     *            must be "Yes I am"
     * @throws IOException
     */
    public static void clearDataBase(final String areYouSure)
            throws IOException {
        if (areYouSure.equals("Yes I am") && db != null) {
            db.close();
            IOHelper.deleteFile(new File(DBPath_));
            db = null;
            keyPrefixes.clear();
        }
    }

    /**
     * Associates the specified value with the specified key in the DB. If the
     * DB previously contained a mapping for the key, the old value is replaced.
     * 
     * @param key
     *            key with which the specified value is to be associated
     * @param value
     *            value to be associated with the specified key
     */
    public void put(final byte[] key, final String value) {
        db.put(key, Serializer.Serialize(value));
    }

    public void put(final String key, final String value) { // String version
        db.put(generateKey(key), Serializer.Serialize(value));
    }

    public void put(final long key, final String value) { // long version
        db.put(generateKey(key), Serializer.Serialize(value));
    }

    public void put(final int key, final String value) { // int version
        db.put(generateKey(key), Serializer.Serialize(value));
    }

    public void put(final short key, final String value) { // short version
        db.put(generateKey(key), Serializer.Serialize(value));
    }

    /**
     * Associates the specified value with the specified key in the DB. If the
     * DB previously contained a mapping for the key, the old value is replaced.
     * 
     * @param key
     *            key with which the specified value is to be associated
     * @param value
     *            value to be associated with the specified key
     */
    public void put(final byte[] key, final long value) {
        db.put(key, Serializer.Serialize(value));
    }

    public void put(final String key, final long value) { // String version
        db.put(generateKey(key), Serializer.Serialize(value));
    }

    public void put(final long key, final long value) { // long version
        db.put(generateKey(key), Serializer.Serialize(value));
    }

    public void put(final int key, final long value) { // int version
        db.put(generateKey(key), Serializer.Serialize(value));
    }

    public void put(final short key, final long value) { // short version
        db.put(generateKey(key), Serializer.Serialize(value));
    }

    /**
     * Associates the specified value with the specified key in the DB. If the
     * DB previously contained a mapping for the key, the old value is replaced.
     * 
     * @param key
     *            key with which the specified value is to be associated
     * @param value
     *            value to be associated with the specified key
     */
    public void put(final byte[] key, final int value) {
        db.put(key, Serializer.Serialize(value));
    }

    public void put(final String key, final int value) { // String version
        db.put(generateKey(key), Serializer.Serialize(value));
    }

    public void put(final long key, final int value) { // long version
        db.put(generateKey(key), Serializer.Serialize(value));
    }

    public void put(final int key, final int value) { // int version
        db.put(generateKey(key), Serializer.Serialize(value));
    }

    public void put(final short key, final int value) { // short version
        db.put(generateKey(key), Serializer.Serialize(value));
    }

    /**
     * Associates the specified value with the specified key in the DB. If the
     * DB previously contained a mapping for the key, the old value is replaced.
     * 
     * @param key
     *            key with which the specified value is to be associated
     * @param value
     *            value to be associated with the specified key
     */
    public void put(final byte[] key, final short value) {
        db.put(key, Serializer.Serialize(value));
    }

    public void put(final String key, final short value) { // String version
        db.put(generateKey(key), Serializer.Serialize(value));
    }

    public void put(final long key, final short value) { // long version
        db.put(generateKey(key), Serializer.Serialize(value));
    }

    public void put(final int key, final short value) { // int version
        db.put(generateKey(key), Serializer.Serialize(value));
    }

    public void put(final short key, final short value) { // short version
        db.put(generateKey(key), Serializer.Serialize(value));
    }

    /**
     * Associates the specified value with the specified key in the DB. If the
     * DB previously contained a mapping for the key, the old value is replaced.
     * 
     * @param key
     *            key with which the specified value is to be associated
     * @param value
     *            value to be associated with the specified key
     */
    public void put(final byte[] key, final boolean value) {
        db.put(key, Serializer.Serialize(value));
    }

    public void put(final String key, final boolean value) { // String version
        db.put(generateKey(key), Serializer.Serialize(value));
    }

    public void put(final long key, final boolean value) { // long version
        db.put(generateKey(key), Serializer.Serialize(value));
    }

    public void put(final int key, final boolean value) { // int version
        db.put(generateKey(key), Serializer.Serialize(value));
    }

    public void put(final short key, final boolean value) { // short version
        db.put(generateKey(key), Serializer.Serialize(value));
    }

    /**
     * Associates the specified value with the specified key in the DB. If the
     * DB previously contained a mapping for the key, the old value is replaced.
     * 
     * @param key
     *            key with which the specified value is to be associated
     * @param value
     *            value to be associated with the specified key
     */
    public void put(final byte[] key, final long[] value) {
        db.put(key, Serializer.Serialize(value));
    }

    public void put(final String key, final long[] value) {
        put(generateKey(key), value);
    }

    public void put(final long key, final long[] value) {
        put(generateKey(key), value);
    }

    public void put(final int key, final long[] value) {
        put(generateKey(key), value);
    }

    public void put(final short key, final long[] value) {
        put(generateKey(key), value);
    }

    /**
     * Adds value to the array associated with the specified key in the DB if it
     * is not already existent (idempotent).
     * 
     * @param key
     *            key associated with the array to which the specified value is
     *            to be added
     * @param value
     *            value to be added to the array
     */
    public void addToSet(final byte[] key, final long value) {
        long[] valueArray = getLongs(key);

        if (valueArray == null) {
            valueArray = new long[1];
        } else {
            /*
             * Check if the long is already stored
             */
            for (long current : valueArray) {
                if (current == value) {
                    return;
                }
            }

            long[] tmp = new long[valueArray.length + 1];
            System.arraycopy(valueArray, 0, tmp, 0, valueArray.length);
            valueArray = tmp;
        }
        valueArray[valueArray.length - 1] = value;
        put(key, valueArray);
    }

    public void addToSet(final String key, final long value) { // String version
        addToSet(generateKey(key), value);
    }

    public void addToSet(final long key, final long value) { // long version
        addToSet(generateKey(key), value);
    }

    public void addToSet(final int key, final long value) { // int version
        addToSet(generateKey(key), value);
    }

    public void addToSet(final short key, final long value) { // short version
        addToSet(generateKey(key), value);
    }

    /**
     * Removes value from the array associated with the specified key in the DB
     * 
     * @param key
     *            key associated with the array from which the specified value
     *            is to be removed
     * @param value
     *            value to be removed from the array
     */
    public boolean removeFromSet(final byte[] key, final long value) {
        long[] valueArray = getLongs(key);

        if (valueArray == null) {
            return false;
        } else {
            /*
             * Seek the element
             */
            int pos = -1;
            for (long current : valueArray) {
                pos++;
                if (current == value) {
                    break;
                }
            }
            if (pos == -1) {
                return false;
            }

            long[] tmp = new long[valueArray.length - 1];
            System.arraycopy(valueArray, 0, tmp, 0, pos);
            System.arraycopy(valueArray, pos + 1, tmp, pos, tmp.length - pos);
            valueArray = tmp;
        }
        put(key, valueArray);
        return true;
    }

    public boolean removeFromSet(final String key, final long value) { // String
        return removeFromSet(generateKey(key), value);
    }

    public boolean removeFromSet(final long key, final long value) { // long
        return removeFromSet(generateKey(key), value);
    }

    public boolean removeFromSet(final int key, final long value) { // int
        return removeFromSet(generateKey(key), value);
    }

    public boolean removeFromSet(final short key, final long value) { // short
        return removeFromSet(generateKey(key), value);
    }

    /**
     * Returns the value to which the specified key is mapped
     * 
     * @param key
     *            The key whose associated value is to be returned
     * @return The value to which the specified key is mapped
     */
    public long getLong(final byte[] key) throws ElementNotFoundException {
        byte[] bytes = db.get(key);
        if (bytes == null) {
            throw new ElementNotFoundException(Arrays.toString(key));
        }
        return (int) Serializer.deserialize(bytes);
    }

    public long getLong(final long key) throws ElementNotFoundException {
        return getInt(generateKey(key));
    }

    public long getLong(final String key) throws ElementNotFoundException {
        return getInt(generateKey(key));
    }

    public long getLong(final int key) throws ElementNotFoundException {
        return getInt(generateKey(key));
    }

    public long getLong(final short key) throws ElementNotFoundException {
        return getInt(generateKey(key));
    }

    /**
     * Returns the value to which the specified key is mapped
     * 
     * @param key
     *            The key whose associated value is to be returned
     * @return The value to which the specified key is mapped
     */
    public int getInt(final byte[] key) throws ElementNotFoundException {
        byte[] bytes = db.get(key);
        if (bytes == null) {
            throw new ElementNotFoundException(Arrays.toString(key));
        }
        return (int) Serializer.deserialize(bytes);
    }

    public int getInt(final long key) throws ElementNotFoundException {
        return getInt(generateKey(key));
    }

    public int getInt(final String key) throws ElementNotFoundException {
        return getInt(generateKey(key));
    }

    public int getInt(final int key) throws ElementNotFoundException {
        return getInt(generateKey(key));
    }

    public int getInt(final short key) throws ElementNotFoundException {
        return getInt(generateKey(key));
    }

    /**
     * Returns the value to which the specified key is mapped
     * 
     * @param key
     *            The key whose associated value is to be returned
     * @return The value to which the specified key is mapped
     */
    public short getShort(final byte[] key) throws ElementNotFoundException {
        byte[] bytes = db.get(key);
        if (bytes == null) {
            throw new ElementNotFoundException(Arrays.toString(key));
        }
        return (short) Serializer.deserialize(bytes);
    }

    public short getShort(final long key) throws ElementNotFoundException {
        return getShort(generateKey(key));
    }

    public short getShort(final String key) throws ElementNotFoundException {
        return getShort(generateKey(key));
    }

    public short getShort(final int key) throws ElementNotFoundException {
        return getShort(generateKey(key));
    }

    public short getShort(final short key) throws ElementNotFoundException {
        return getShort(generateKey(key));
    }

    /**
     * Returns the value to which the specified key is mapped
     * 
     * @param key
     *            The key whose associated value is to be returned
     * @return The value to which the specified key is mapped
     */
    public boolean getBoolean(final byte[] key) throws ElementNotFoundException {
        byte[] bytes = db.get(key);
        if (bytes == null) {
            throw new ElementNotFoundException(Arrays.toString(key));
        }
        return (boolean) Serializer.deserialize(bytes);
    }

    public boolean getBoolean(final long key) throws ElementNotFoundException {
        return getBoolean(generateKey(key));
    }

    public boolean getBoolean(final String key) throws ElementNotFoundException {
        return getBoolean(generateKey(key));
    }

    public boolean getBoolean(final int key) throws ElementNotFoundException {
        return getBoolean(generateKey(key));
    }

    public boolean getBoolean(final short key) throws ElementNotFoundException {
        return getBoolean(generateKey(key));
    }

    /**
     * Returns the value to which the specified key is mapped
     * 
     * @param key
     *            The key whose associated value is to be returned
     * @return The value to which the specified key is mapped
     */
    public String getString(final byte[] key) {
        return (String) Serializer.deserialize(db.get(key));
    }

    public String getString(final long key) {
        return getString(generateKey(key));
    }

    public String getString(final String key) {
        return getString(generateKey(key));
    }

    public String getString(final int key) {
        return getString(generateKey(key));
    }

    public String getString(final short key) {
        return getString(generateKey(key));
    }

    /**
     * Returns the long[] to which the specified key is mapped, or null if the
     * DB contains no mapping for the key.
     * 
     * @param key
     *            The key whose associated value is to be returned
     * @return The long[] to which the specified key is mapped, or null if the
     *         DB contains no mapping for the key.
     */
    public long[] getLongs(final byte[] key) {
        byte[] bytes = db.get(key);
        if (bytes == null) {
            return null;
        }
        return (long[]) Serializer.deserialize(bytes);
    }

    public long[] getLongs(final String key) {
        return getLongs(generateKey(key));
    }

    public long[] getLongs(final long key) {
        return getLongs(generateKey(key));
    }

    public long[] getLongs(final int key) {
        return getLongs(generateKey(key));
    }

    public long[] getLongs(final short key) {
        return getLongs(generateKey(key));
    }

    /**
     * Removes the mapping for a key from this DB if it is present
     * 
     * @param keyUUID
     *            The key to be removed
     */
    public void removeKey(final String keyUUID) {
        db.delete(generateKey(keyUUID));
    }

    public void removeKey(final long keyUUID) {
        db.delete(generateKey(keyUUID));
    }

    public void removeKey(final int keyUUID) {
        db.delete(generateKey(keyUUID));
    }

    public void removeKey(final short keyUUID) {
        db.delete(generateKey(keyUUID));
    }

    /**
     * Try to avoid using this method and use get() instead!
     * 
     * @param keyUUID
     * @return
     */
    public boolean containsKey(final String keyUUID) {
        return db.get(generateKey(keyUUID)) != null;
    }

    public boolean containsKey(final long keyUUID) {
        return db.get(generateKey(keyUUID)) != null;
    }

    public boolean containsKey(final int keyUUID) {
        return db.get(generateKey(keyUUID)) != null;
    }

    public boolean containsKey(final short keyUUID) {
        return db.get(generateKey(keyUUID)) != null;
    }

    /**
     * Try to avoid using this method and use get() instead!
     * 
     * FIXME: Sort the Set and use binary search
     * 
     * @param key
     * @return
     */
    public boolean setContainsElement(final byte[] key, final long value) {
        if (getLongs(key) == null) {
            return false;
        }
        for (long l : getLongs(key)) {
            if (l == value) {
                return true;
            }
        }
        return false;
    }

    public boolean setContainsElement(final String key, final long value) {
        return setContainsElement(generateKey(key), value);
    }

    public boolean setContainsElement(final long key, final long value) {
        return setContainsElement(generateKey(key), value);
    }

    public boolean setContainsElement(final int key, final long value) {
        return setContainsElement(generateKey(key), value);
    }

    public boolean setContainsElement(final short key, final long value) {
        return setContainsElement(generateKey(key), value);
    }

    /**
     * Generates a key identifying the given keySuffix within this instance
     * 
     * @param keySuffix
     *            the key that should be concatenated to the key identifying
     *            this instance
     * @return the key identifying keySuffix within this instance
     */
    public byte[] generateKey(final String keySuffix) {
        if (keySuffix.length() == 0) {
            return null;
        }
        byte[] suffix = JniDBFactory.bytes(keySuffix);
        byte[] key = new byte[8 + keySuffix.length()];
        System.arraycopy(keyPrefix, 0, key, 0, 8);
        System.arraycopy(suffix, 0, key, 8, suffix.length);

        return key;
    }

    /**
     * Generates a key identifying the given keySuffix within this instance
     * 
     * @param keySuffix
     *            the key that should be concatenated to the key identifying
     *            this instance
     * @return the key identifying keySuffix within this instance
     */
    public byte[] generateKey(final long keySuffix) {
        byte[] key = new byte[16];
        System.arraycopy(keyPrefix, 0, key, 0, 8);
        key[8] = (byte) (keySuffix >> 56);
        key[9] = (byte) (keySuffix >> 48);
        key[10] = (byte) (keySuffix >> 40);
        key[11] = (byte) (keySuffix >> 32);
        key[12] = (byte) (keySuffix >> 24);
        key[13] = (byte) (keySuffix >> 16);
        key[14] = (byte) (keySuffix >> 8);
        key[15] = (byte) (keySuffix);

        return key;
    }

    /**
     * Generates a key identifying the given keySuffix within this instance
     * 
     * @param keySuffix
     *            the key that should be concatenated to the key identifying
     *            this instance
     * @return the key identifying keySuffix within this instance
     */
    public byte[] generateKey(final int keySuffix) {
        byte[] key = new byte[12];
        System.arraycopy(keyPrefix, 0, key, 0, 8);
        key[8] = (byte) (keySuffix >> 24);
        key[9] = (byte) (keySuffix >> 16);
        key[10] = (byte) (keySuffix >> 8);
        key[11] = (byte) (keySuffix);

        return key;
    }

    /**
     * Generates a key identifying the given keySuffix within this instance
     * 
     * @param keySuffix
     *            the key that should be concatenated to the key identifying
     *            this instance
     * @return the key identifying keySuffix within this instance
     */
    public byte[] generateKey(final short keySuffix) {
        byte[] key = new byte[10];
        System.arraycopy(keyPrefix, 0, key, 0, 8);
        key[8] = (byte) (keySuffix >> 8);
        key[9] = (byte) (keySuffix);

        return key;
    }

    @Override
    public String toString() {
        if (db == null) {
            return "DB is Empty";
        }
        StringBuilder builder = new StringBuilder();
        DBIterator iterator = db.iterator();
        for (iterator.seekToFirst(); iterator.hasNext(); iterator.next()) {
            System.out.println(asString(iterator.peekNext().getKey()) + ":");
            builder.append(asString(iterator.peekNext().getKey()) + ":");
            builder.append("\t"
                    + Arrays.toString((long[]) Serializer.deserialize(iterator
                            .peekNext().getValue())));
        }
        try {
            iterator.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return builder.toString();
    }
}
