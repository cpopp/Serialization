package com.cp.serialization;


import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Date;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class PersistedMetaDataSerializerTest {

	// create the in memory data store backing the persistent serializer
	private DataStore dataStore;
	
	// serializer itself that will persist and lookup metadata in the store
	private Serializer serializer;
	
	@Before
	public void setupSerializer() {
		dataStore = new InMemoryDataStore();
		serializer = new PersistedMetaDataSerializer(dataStore);
	}
	
	/**
	 * Tests a simple pojo class that just has a string field
	 */
	@Test
	public void testSimpleSerialization() throws Exception {
		SimplePojo pojo = new SimplePojo();
		pojo.setContent("small");
		
		// try serializing and deserializing the pojo
		byte[] smallPayload = serializer.serialize(pojo);
		SimplePojo deserialized = (SimplePojo)serializer.deserialize(smallPayload);
		Assert.assertEquals("small", deserialized.getContent());
		
		Assert.assertTrue(deserialized instanceof SimplePojo);
		
		// give the pojo a larger payload.  the serialized payload
		// should be larger given the longer string
		pojo.setContent("something larger");
		byte[] largerPayload = serializer.serialize(pojo);
		Assert.assertTrue(largerPayload.length > smallPayload.length);
	}
	
	/**
	 * Tests a complex pojo that has the supported types plus one
	 * field that is not a supported type
	 */
	@Test
	public void testComplexSerialization() throws Exception {
		SimplePojo simplePojo = new SimplePojo();
		simplePojo.setContent("some string");
		
		// try a pojo that has mostly default values for the types, and empty arrays
		ComplexPojo complexPojo = new ComplexPojo(false, (byte)0, (short)0, 0, 0L,
				0f, 0.0, null, Boolean.FALSE, (byte)0, (short)0, (int)0, (long)0,
				(Float)0f, (Double)0.0, new boolean[0], new byte[0], new short[0], new int[0],
				new long[0], new float[0], new double[0], "string", new Date(0L),
				BigDecimal.ZERO, simplePojo);
		
		byte[] complexPojoPayload = serializer.serialize(complexPojo);
		
		ComplexPojo deserializedComplexPojo = (ComplexPojo)serializer.deserialize(complexPojoPayload);
		Assert.assertEquals(complexPojo, deserializedComplexPojo);
		
		// if that passed, try one with non-default values and arrays with data
		complexPojo = new ComplexPojo(true, Byte.MIN_VALUE, Short.MAX_VALUE, Integer.MIN_VALUE, Long.MAX_VALUE,
				Float.MIN_VALUE, Double.MAX_VALUE, null, Boolean.TRUE, Byte.MAX_VALUE, Short.MIN_VALUE, Integer.MAX_VALUE, Long.MIN_VALUE,
				Float.MAX_VALUE, -Double.MAX_VALUE, new boolean[]{false, true}, new byte[]{-1, 0, 1}, new short[]{-1435, 2345}, new int[]{234, 529349},
				new long[]{Long.MIN_VALUE, 123}, new float[]{-Float.MAX_VALUE, 123}, new double[]{-Double.MIN_VALUE,1.1f}, "~!@#$%^&*()_+`1234567890-={}|[]\\:\",./<?>", new Date(),
				new BigDecimal(42387293948234L), simplePojo);
		
		complexPojoPayload = serializer.serialize(complexPojo);
		
		deserializedComplexPojo = (ComplexPojo)serializer.deserialize(complexPojoPayload);
		Assert.assertEquals(complexPojo, deserializedComplexPojo);
	}

	private static class SimplePojo {
		private String content;
		
		public void setContent(String content) {
			this.content = content;
		}
		
		public String getContent() {
			return content;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result
					+ ((content == null) ? 0 : content.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			SimplePojo other = (SimplePojo) obj;
			if (content == null) {
				if (other.content != null)
					return false;
			} else if (!content.equals(other.content))
				return false;
			return true;
		}
		
		
	}
	
	private static class ComplexPojo {
		public ComplexPojo() {
			
		}
		
		public ComplexPojo(boolean booleanField, byte byteField,
				short shortField, int intField, long longField,
				float floatField, double doubleField, String nullStringField,
				Boolean booleanWrapperField, Byte byteWrapperField,
				Short shortWrapperField, Integer intWrapperField,
				Long longWrapperField, Float floatWrapperField,
				Double doubleWrapperField, boolean[] booleanArrayField,
				byte[] byteArrayField, short[] shortArrayField,
				int[] intArrayField, long[] longArrayField,
				float[] floatArrayField, double[] doubleArrayField,
				String stringField, Date dateField, BigDecimal bigDecimalField,
				SimplePojo simplePojo) {

			this.booleanField = booleanField;
			this.byteField = byteField;
			this.shortField = shortField;
			this.intField = intField;
			this.longField = longField;
			this.floatField = floatField;
			this.doubleField = doubleField;
			this.nullStringField = nullStringField;
			this.booleanWrapperField = booleanWrapperField;
			this.byteWrapperField = byteWrapperField;
			this.shortWrapperField = shortWrapperField;
			this.intWrapperField = intWrapperField;
			this.longWrapperField = longWrapperField;
			this.floatWrapperField = floatWrapperField;
			this.doubleWrapperField = doubleWrapperField;
			this.booleanArrayField = booleanArrayField;
			this.byteArrayField = byteArrayField;
			this.shortArrayField = shortArrayField;
			this.intArrayField = intArrayField;
			this.longArrayField = longArrayField;
			this.floatArrayField = floatArrayField;
			this.doubleArrayField = doubleArrayField;
			this.stringField = stringField;
			this.dateField = dateField;
			this.bigDecimalField = bigDecimalField;
			this.simplePojo = simplePojo;
		}
		protected boolean booleanField;
		protected byte byteField;
		protected short shortField;
		protected int intField;
		protected long longField;
		protected float floatField;
		protected double doubleField;
		
		protected String nullStringField;
		
		protected Boolean booleanWrapperField;
		protected Byte byteWrapperField;
		protected Short shortWrapperField;
		protected Integer intWrapperField;
		protected Long longWrapperField;
		protected Float floatWrapperField;
		protected Double doubleWrapperField;
		
		protected boolean[] booleanArrayField;
		protected byte[] byteArrayField;
		protected short[] shortArrayField;
		protected int[] intArrayField;
		protected long[] longArrayField;
		protected float[] floatArrayField;
		protected double[] doubleArrayField;
		
		protected String stringField;
		protected Date dateField;
		protected BigDecimal bigDecimalField;
		protected SimplePojo simplePojo;
		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime
					* result
					+ ((bigDecimalField == null) ? 0 : bigDecimalField
							.hashCode());
			result = prime * result + Arrays.hashCode(booleanArrayField);
			result = prime * result + (booleanField ? 1231 : 1237);
			result = prime
					* result
					+ ((booleanWrapperField == null) ? 0 : booleanWrapperField
							.hashCode());
			result = prime * result + Arrays.hashCode(byteArrayField);
			result = prime * result + byteField;
			result = prime
					* result
					+ ((byteWrapperField == null) ? 0 : byteWrapperField
							.hashCode());
			result = prime * result
					+ ((dateField == null) ? 0 : dateField.hashCode());
			result = prime * result + Arrays.hashCode(doubleArrayField);
			long temp;
			temp = Double.doubleToLongBits(doubleField);
			result = prime * result + (int) (temp ^ (temp >>> 32));
			result = prime
					* result
					+ ((doubleWrapperField == null) ? 0 : doubleWrapperField
							.hashCode());
			result = prime * result + Arrays.hashCode(floatArrayField);
			result = prime * result + Float.floatToIntBits(floatField);
			result = prime
					* result
					+ ((floatWrapperField == null) ? 0 : floatWrapperField
							.hashCode());
			result = prime * result + Arrays.hashCode(intArrayField);
			result = prime * result + intField;
			result = prime
					* result
					+ ((intWrapperField == null) ? 0 : intWrapperField
							.hashCode());
			result = prime * result + Arrays.hashCode(longArrayField);
			result = prime * result + (int) (longField ^ (longField >>> 32));
			result = prime
					* result
					+ ((longWrapperField == null) ? 0 : longWrapperField
							.hashCode());
			result = prime
					* result
					+ ((nullStringField == null) ? 0 : nullStringField
							.hashCode());
			result = prime * result + Arrays.hashCode(shortArrayField);
			result = prime * result + shortField;
			result = prime
					* result
					+ ((shortWrapperField == null) ? 0 : shortWrapperField
							.hashCode());
			result = prime * result
					+ ((simplePojo == null) ? 0 : simplePojo.hashCode());
			result = prime * result
					+ ((stringField == null) ? 0 : stringField.hashCode());
			return result;
		}
		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			ComplexPojo other = (ComplexPojo) obj;
			if (bigDecimalField == null) {
				if (other.bigDecimalField != null)
					return false;
			} else if (!bigDecimalField.equals(other.bigDecimalField))
				return false;
			if (!Arrays.equals(booleanArrayField, other.booleanArrayField))
				return false;
			if (booleanField != other.booleanField)
				return false;
			if (booleanWrapperField == null) {
				if (other.booleanWrapperField != null)
					return false;
			} else if (!booleanWrapperField.equals(other.booleanWrapperField))
				return false;
			if (!Arrays.equals(byteArrayField, other.byteArrayField))
				return false;
			if (byteField != other.byteField)
				return false;
			if (byteWrapperField == null) {
				if (other.byteWrapperField != null)
					return false;
			} else if (!byteWrapperField.equals(other.byteWrapperField))
				return false;
			if (dateField == null) {
				if (other.dateField != null)
					return false;
			} else if (!dateField.equals(other.dateField))
				return false;
			if (!Arrays.equals(doubleArrayField, other.doubleArrayField))
				return false;
			if (Double.doubleToLongBits(doubleField) != Double
					.doubleToLongBits(other.doubleField))
				return false;
			if (doubleWrapperField == null) {
				if (other.doubleWrapperField != null)
					return false;
			} else if (!doubleWrapperField.equals(other.doubleWrapperField))
				return false;
			if (!Arrays.equals(floatArrayField, other.floatArrayField))
				return false;
			if (Float.floatToIntBits(floatField) != Float
					.floatToIntBits(other.floatField))
				return false;
			if (floatWrapperField == null) {
				if (other.floatWrapperField != null)
					return false;
			} else if (!floatWrapperField.equals(other.floatWrapperField))
				return false;
			if (!Arrays.equals(intArrayField, other.intArrayField))
				return false;
			if (intField != other.intField)
				return false;
			if (intWrapperField == null) {
				if (other.intWrapperField != null)
					return false;
			} else if (!intWrapperField.equals(other.intWrapperField))
				return false;
			if (!Arrays.equals(longArrayField, other.longArrayField))
				return false;
			if (longField != other.longField)
				return false;
			if (longWrapperField == null) {
				if (other.longWrapperField != null)
					return false;
			} else if (!longWrapperField.equals(other.longWrapperField))
				return false;
			if (nullStringField == null) {
				if (other.nullStringField != null)
					return false;
			} else if (!nullStringField.equals(other.nullStringField))
				return false;
			if (!Arrays.equals(shortArrayField, other.shortArrayField))
				return false;
			if (shortField != other.shortField)
				return false;
			if (shortWrapperField == null) {
				if (other.shortWrapperField != null)
					return false;
			} else if (!shortWrapperField.equals(other.shortWrapperField))
				return false;
			if (simplePojo == null) {
				if (other.simplePojo != null)
					return false;
			} else if (!simplePojo.equals(other.simplePojo))
				return false;
			if (stringField == null) {
				if (other.stringField != null)
					return false;
			} else if (!stringField.equals(other.stringField))
				return false;
			return true;
		}
		
		
	}
}
