package com.cp.serialization;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Example data store for serialization metadata that can
 * be used for testing purposes.  Other than testing it should
 * be used carefully as you will only be able to deserialize
 * data that serialized using a particular reference of the
 * memory data store.
 */
public class InMemoryDataStore implements DataStore {
	private final Map<String, byte[]> store = new HashMap<String, byte[]>();
	private final AtomicLong counter = new AtomicLong();
	
	@Override
	public void storeData(String key, byte[] data) {
		store.put(key, data);
	}

	@Override
	public byte[] loadData(String key) {
		return store.get(key);
	}

	@Override
	public long nextCounterValue() {
		return counter.getAndIncrement();
	}
}
