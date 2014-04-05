package de.metalcon.dbhelper;

import de.metalcon.exceptions.MetalconException;

public class ElementNotFoundException extends MetalconException {

	private static final long serialVersionUID = -1653678857749915082L;

	public ElementNotFoundException(Object key) {
		super("The element with the key " + key
				+ " could not be found in the database");
	}
}
