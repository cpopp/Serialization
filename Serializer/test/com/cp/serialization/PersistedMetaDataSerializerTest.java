package com.cp.serialization;


import org.junit.Assert;
import org.junit.Test;

public class PersistedMetaDataSerializerTest {

	@Test
	public void testSimpleSerialization() throws Exception {
		// create the in memory data store backing the persistent serializer
		InMemoryDataStore dataStore = new InMemoryDataStore();
		
		// serializer itself that will persist and lookup metadata in the store
		Serializer serializer = new PersistedMetaDataSerializer(dataStore);
		
		SimplePojo pojo = new SimplePojo();
		pojo.setContent("small");
		
		// try serializing and deserializing the pojo
		byte[] smallPayload = serializer.serialize(pojo);
		Object deserialized = serializer.deserialize(smallPayload);
		Assert.assertTrue(deserialized instanceof SimplePojo);
		
		// give the pojo a larger payload.  the serialized payload
		// should be larger given the longer string
		pojo.setContent("something larger");
		byte[] largerPayload = serializer.serialize(pojo);
		Assert.assertTrue(largerPayload.length > smallPayload.length);
	}

	private static class SimplePojo {
		private String content;
		
		public void setContent(String content) {
			this.content = content;
		}
	}
}
