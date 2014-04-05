package de.metalcon.dbhelper;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;

import org.apache.commons.lang3.SerializationUtils;

import de.metalcon.exceptions.MetalconRuntimeException;

public class Serializer {

	/**
	 * Used to serialize any objects that should be stored in the DB
	 * 
	 * @param obj
	 *            the object to be serialized
	 * @return the serialized object
	 */
	public static byte[] Serialize(Object obj) {
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
	 * Used to serialize any objects that should be stored in the DB
	 * 
	 * @param obj
	 *            the object to be serialized
	 * @return the serialized object
	 */
	public static byte[] Serialize(Serializable obj) {
		return SerializationUtils.serialize(obj);
	}

	/**
	 * Used to serialize ints that should be stored in the DB
	 * 
	 * @param obj
	 *            the object to be serialized
	 * @return the serialized object
	 */
	public static byte[] Serialize(int i) {
		ByteBuffer dbuf = ByteBuffer.allocate(4);
		dbuf.putInt(i);
		return dbuf.array();
	}

	/**
	 * Used to serialize shorts that should be stored in the DB
	 * 
	 * @param obj
	 *            the object to be serialized
	 * @return the serialized object
	 */
	public static byte[] Serialize(short i) {
		ByteBuffer dbuf = ByteBuffer.allocate(2);
		dbuf.putShort(i);
		return dbuf.array();
	}

	/**
	 * Used to deserialize any object stored in the DB
	 * 
	 * @param obj
	 *            the serialized object
	 * @return the deserialized object
	 */
	public static Object deserialize(byte[] obj) {
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
}
