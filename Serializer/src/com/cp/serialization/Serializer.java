package com.cp.serialization;

import java.io.IOException;

public interface Serializer {
	/**
	 * Create a compact binary represent of the supplied object
	 */
	public byte[] serialize(Object toSerialize) throws IOException;
	
	/**
	 * Returns an object based on previously serialized data
	 */
	public Object deserialize(byte[] data) throws Exception;
}
