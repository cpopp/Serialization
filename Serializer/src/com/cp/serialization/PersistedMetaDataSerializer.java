package com.cp.serialization;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;

import com.cp.serialization.ClassMetaData.FieldInfo;

/**
 * A serializer that uses a data store to save metadata about the class
 * it is serializing in order to provide a more compact representation
 * of the class.  The life of the data stored by the data store must
 * be as long as you wish to be able to deserialize data for.<p>
 * 
 * In order to minimize the amount of data consumed when objects reference
 * each other, the serializer uses the data store to maintain an incrementing
 * counter that it uses to map new metadata keys to a more compact identifier.<p>
 * 
 * For example, if you are only going to serialize data within memory
 * of application, it would be fine to use a single instance of the
 * {@link InMemoryDataStore} data store, because serialized data
 * would disappear along with the in memory data store.  However,
 * if you store serialized data somewhere, you would need to provide
 * an implementation of a DataStore that stores the meta data for
 * just as long.<p>
 * 
 * Similarly, if you will be transmitting serialized data across machines,
 * the backing for the data store needs to be shared across nodes. A 
 * reasonable implementation of a DataStore would be a database with
 * a cache in each application providing data for 
 * {@link DataStore#loadData(String).  This way, any member of the cluster
 * will instantly see new metadata, but as entries do not change, almost
 * everything will hit the cache for the metadata.
 */
public class PersistedMetaDataSerializer implements Serializer {
	/**
	 * Used to store and lookup metadata that is used to serialize
	 * objects in a compact manner
	 */
	final DataStore dataStore;
	
	public PersistedMetaDataSerializer(DataStore dataStore) {
		this.dataStore = dataStore;
	}

	/**
	 * Serializes the declared fields of the supplied object,
	 * so long as they are not final, static, or transient.<p>
	 * 
	 * Due to the restriction on the type of fields serialized,
	 * this class is primarily useful for simple custom objects 
	 * with a controlled object graph.<p>
	 * 
	 * First we build a key for the metadata which is the
	 * {className}/{hash} where the hash is a SHA-1 over
	 * its field names and types.  If the data store already
	 * contains the metadata for this we proceed.  If it does
	 * not, we ask the data store for a new identifier, and then
	 * store a compact identifier in the data store that maps
	 * to the key we've selected.  Then we store the meta data
	 * itself by the key.<p>
	 * 
	 * We then proceed to serialize the object.  The first thing
	 * we do is write out the object's compact id.  From there,
	 * we write compact representations for common types.  If
	 * an object is encountered that we do not have a special
	 * mapping for, we generate metadata for that object as
	 * described above in order to serialize it concisely.
	 */
	@Override
	public byte[] serialize(Object toSerialize) throws IOException {
		// build metadata for the supplied object's class
		ClassMetaData metadata = new ClassMetaData(toSerialize.getClass());
		
		// check to see if the data store already has an entry for this class
		byte[] existingMetadata = dataStore.loadData(metadata.getKey());
		if(existingMetadata == null) {
			// if not, get the next available compact identifier
			metadata.setCompactId(dataStore.nextCounterValue());
			// save off the compact id (compactId -> key)
			try {
				dataStore.storeData(Long.toString(metadata.getCompactId()), metadata.getKey().getBytes("UTF-8"));
			} catch (UnsupportedEncodingException e) {
				throw new IllegalStateException("Expected UTF-8 to be available");
			}
			// and then save off the meta data (key -> metadata)
			dataStore.storeData(metadata.getKey(), metadata.toByteArray());
		} else {
			// use the existing metadata to get information such as the compactId
			metadata = new ClassMetaData(metadata.getKey(), existingMetadata);
		}
		
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		BitOutputStream bos = new BitOutputStream(baos);
		
		try {
			bos.writeDynamicNumber(metadata.getCompactId());
			for(FieldInfo fieldInfo : metadata.getFields()) {
				Field field = toSerialize.getClass().getDeclaredField(fieldInfo.getName());
				field.setAccessible(true);
				writeValue(bos, field.getType(), field.get(toSerialize));
			}
			
			bos.flush();
		} catch (NoSuchFieldException e) {
			throw new IllegalStateException("Field looked up previously", e);
		} catch (IllegalAccessException e) {
			throw new IllegalStateException("Expected no SecurityManager", e);
		} catch (IOException e) {
			throw new IllegalStateException("Unexpected IOException writing to ByteArrayOutputStream", e);
		}
		
		
		
		return baos.toByteArray();
	}
	
	/**
	 * Deserializes the supplied payload into an object, throwing an Exception
	 * if the data is corrupt, we can't find the class, or the class does not
	 * have a no argument constructor.<p>
	 * 
	 * It is possible the class may be different than the one that was 
	 * serialized.  If fields have been re-ordering, they will still be
	 * set.  If fields no longer exist or their types have changed, the
	 * serialized data for the field will be discarded.  If new fields
	 * exist, they will retain their default value.<p>
	 * 
	 * It is planned to pass off handling for the above conditions to
	 * an optional listener, but it is now implemented at this point.<p>
	 * 
	 * TODO: Actually implement setting of fields...right now deserialize
	 * only instantiates the object based on looking up the compactId/metadata
	 * from the data store...it just needs to implement reading types from the
	 * supplied data now
	 */
	@Override
	public Object deserialize(byte[] data) throws Exception {
		BitInputStream bis = new BitInputStream(new ByteArrayInputStream(data));
		String compactId = Long.toString(bis.readDynamicNumber());
		
		byte[] keyBytes = dataStore.loadData(compactId);
		if(keyBytes == null) {
			throw new IllegalArgumentException("No key found for " + compactId);
		}
		String key = new String(keyBytes, "UTF-8");
		
		byte[] classMetaData = dataStore.loadData(key);
		if(classMetaData == null) {
			throw new IllegalArgumentException("No metadata found for " + key);
		}
		
		ClassMetaData metaData = new ClassMetaData(key, classMetaData);
		Class<?> clazz = Thread.currentThread().getContextClassLoader().loadClass(metaData.getClassName());
		
		Constructor<?> constructor = clazz.getDeclaredConstructor();
		constructor.setAccessible(true);
		Object deserialized = constructor.newInstance();
		
		// TODO: deserialization logic
		
		return deserialized;
	}

	/**
	 * Writes the supplied value to the stream.  This handles the primitive types
	 * and for objects, writes a boolean indicating whether the object is null,
	 * and if it is not null, it passes it off to 
	 * {@link #writeObject(BitOutputStream, Class, Object)} for further writing
	 */
	private void writeValue(BitOutputStream bos, Class<?> type, Object value) throws IOException {
		DataOutputStream dos = new DataOutputStream(bos);
		
		if(type.equals(Boolean.TYPE)) { // primitive types
			bos.writeBoolean((Boolean)value);
		} else if(type.equals(Short.TYPE)) {
			bos.writeDynamicNumber((Short)value);
		} else if(type.equals(Integer.TYPE)) {
			bos.writeDynamicNumber((Integer)value);
		} else if(type.equals(Long.TYPE)){
			dos.writeLong((Long)value);
		} else if(type.equals(Float.TYPE)) {
			dos.writeFloat((Float)value);
		} else if(type.equals(Double.TYPE)) {
			dos.writeDouble((Double)value);
		} else {
			// not a primitive...write false for null, otherwise true then value
			if(value == null) {
				bos.writeBoolean(false);
			} else {
				bos.writeBoolean(true);
				writeObject(bos, type, value);
			}
		}
	}
	
	/**
	 * Writes the supplied object to the stream, presuming that sufficient
	 * information has been written to determine that the object is not null
	 */
	private void writeObject(BitOutputStream bos, Class<?> type, Object value) throws IOException {
		DataOutputStream dos = new DataOutputStream(bos);
		
		if(type.equals(Boolean.class)) { // auto-boxed primitives
			bos.writeBoolean((Boolean)value);
		} else if(type.equals(Short.class)) {
			bos.writeDynamicNumber((Short)value);
		} else if(type.equals(Integer.class)) {
			bos.writeDynamicNumber((Integer)value);
		} else if(type.equals(Long.class)) {
			dos.writeLong((Long)value);
		} else if(type.equals(Float.class)) {
			dos.writeFloat((Float)value);
		} else if(type.equals(Double.class)) {
			dos.writeDouble((Double)value);
		} else if(type.equals(boolean[].class)) { // primitive arrays
			boolean[] array = (boolean[])value;
			bos.writeDynamicNumber(array.length);
			for(boolean b : array) {
				dos.writeBoolean(b);
			}
		} else if(type.equals(byte[].class)) { 
			byte[] array = (byte[])value;
			bos.writeDynamicNumber(array.length);
			for(byte b : array) {
				dos.writeByte(b);
			}
		} else if(type.equals(short[].class)) {
			short[] array = (short[])value;
			bos.writeDynamicNumber(array.length);
			for(short s : array) {
				dos.writeShort(s);
			}
		} else if(type.equals(int[].class)) {
			int[] array = (int[])value;
			bos.writeDynamicNumber(array.length);
			for(int i : array) {
				dos.writeInt(i);
			}
		} else if(type.equals(long[].class)) {
			long[] array = (long[])value;
			bos.writeDynamicNumber(array.length);
			for(long l : array) {
				dos.writeLong(l);
			}
		} else if(type.equals(float[].class)) {
			float[] array = (float[])value;
			bos.writeDynamicNumber(array.length);
			for(float f : array) {
				dos.writeFloat(f);
			}
		} else if(type.equals(double[].class)) {
			double[] array = (double[])value;
			bos.writeDynamicNumber(array.length);
			for(double d : array) {
				dos.writeDouble(d);
			}
		} else if(type.equals(String.class)) { // String
			bos.writeUTF((String)value, false); 
		} else if(type.equals(Date.class)){ // Date
			dos.writeLong(((Date)value).getTime());
		} else if(type.equals(BigDecimal.class)) { // BigDecimal
			BigDecimal bigDecimal = (BigDecimal)value;
			BigInteger unscaledValue = bigDecimal.unscaledValue();
			int scale = bigDecimal.scale();
			writeObject(bos, byte[].class, unscaledValue.toByteArray());
			writeValue(bos, Integer.TYPE, scale);
		} else {
			// hmm not sure what type it is.  generated its metadata and
			// serialize it like anything else.  due to the compactId
			// we assign to each new type, the type information won't
			// be overwhelming
			byte[] data = serialize(value);
			writeObject(bos, byte[].class, data);
		}
	}
}
