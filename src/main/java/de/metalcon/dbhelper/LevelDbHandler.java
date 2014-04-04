package de.metalcon.dbhelper;

import static org.fusesource.leveldbjni.JniDBFactory.asString;
import static org.fusesource.leveldbjni.JniDBFactory.factory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.HashSet;

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
	public LevelDbHandler(final long keyPrefix) {
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
	public LevelDbHandler(final String keyPrefix) {
		this(keyPrefix.hashCode() + 0xFFFFFFFFL * keyPrefix.hashCode());
	}

	/**
	 * Completely deletes all data stored in the central levelDB (including all
	 * data from all instances
	 * 
	 * @param areYouSure
	 *            must be "Yes I am"
	 * @throws IOException
	 */
	public static void clearDataBase(String areYouSure) throws IOException {
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
	public void put(final String key, final int value) {
		db.put(generateKey(key), Serialize(value));
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
	public void put(final long key, final long value) {
		db.put(generateKey(key), Serialize(value));
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

	/**
	 * @see LevelDbHandler.setAdd(final byte[] key, final long value)
	 */
	public void addToSet(final long key, final long value) {
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

	/**
	 * @see LevelDbHandler.removeFromSet(final byte[] key, final long value)
	 */
	public boolean removeFromSet(long key, final long value) {
		return removeFromSet(generateKey(key), value);
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
		db.put(key, Serialize(value));
	}

	/**
	 * Returns the integer to which the specified key is mapped, or
	 * Integer.MIN_VALUE if the DB contains no mapping for the key.
	 * 
	 * @param key
	 *            The key whose associated value is to be returned
	 * @return The integer to which the specified key is mapped, or
	 *         Integer.MIN_VALUE if the DB contains no mapping for the key.
	 */
	public int getInt(final String key) {
		try {
			byte[] bytes = db.get(generateKey(key.hashCode()));
			if (bytes == null) {
				return Integer.MIN_VALUE;
			}
			return (int) DeSerialize(bytes);
		} catch (final NumberFormatException e) {
			return 0;
		}
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
	public long[] getLongs(final long key) {
		return getLongs(generateKey(key));
	}

	/**
	 * @see LevelDbHandler.getLongs(final long key)
	 */
	public long[] getLongs(final byte[] key) {
		byte[] bytes = db.get(key);
		if (bytes == null) {
			return null;
		}
		return (long[]) DeSerialize(bytes);
	}

	/**
	 * Removes the mapping for a key from this DB if it is present
	 * 
	 * @param keyUUID
	 *            The key to be removed
	 */
	public void removeKey(final long keyUUID) {
		db.delete(generateKey(keyUUID));
	}

	/**
	 * Try to avoid using this method and use get() instead!
	 * 
	 * @param keyUUID
	 * @return
	 */
	public boolean containsKey(final long keyUUID) {
		return db.get(generateKey(keyUUID)) != null;
	}

	/**
	 * Try to avoid using this method and use get() instead!
	 * 
	 * FIXME: Sort the Set and use binary search
	 * 
	 * @param keyUUID
	 * @return
	 */
	public boolean setContainsElement(final byte[] keyUUID, final long valueUUID) {
		if (getLongs(keyUUID) == null) {
			return false;
		}
		for (long l : getLongs(keyUUID)) {
			if (l == valueUUID) {
				return true;
			}
		}
		return false;
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
		return generateKey(keySuffix.hashCode());
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
	 * Used to serialize any objects that should be stored in the DB
	 * 
	 * @param obj
	 *            the object to be serialized
	 * @return the serialized object
	 */
	private static byte[] Serialize(Object obj) {
		byte[] out = null;
		if (obj != null) {
			try {
				ByteArrayOutputStream baos = new ByteArrayOutputStream();
				ObjectOutputStream oos = new ObjectOutputStream(baos);
				oos.writeObject(obj);
				out = baos.toByteArray();
			} catch (IOException e) {
				e.printStackTrace();
				return null;
			}
		}
		return out;
	}

	/**
	 * Used to deserialize any object stored in the DB
	 * 
	 * @param obj
	 *            the serialized object
	 * @return the deserialized object
	 */
	private static Object DeSerialize(byte[] obj) {
		Object out = null;
		if (obj != null) {
			try {
				ByteArrayInputStream bios = new ByteArrayInputStream(obj);
				ObjectInputStream ois = new ObjectInputStream(bios);
				out = ois.readObject();
			} catch (Exception e) {
				throw new MetalconRuntimeException(e.getMessage());
			}
		}
		return out;
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
					+ Arrays.toString((long[]) DeSerialize(iterator.peekNext()
							.getValue())));
		}
		try {
			iterator.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return builder.toString();
	}
}
