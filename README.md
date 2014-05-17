Persistent Metadata Serialization
=========

An experimental serializer with a design that lies somewhere between built-in serialization and something like a custom writeExternal method for each object in terms of flexibility and compactness, with the additioan of generating metadata that is saved out of band of the serialized payload.  (TODO: finish off deserialization code)

Basic Approach
----
The serializer attempts to automatically serialize classes by using reflection to find fields that are not static, final, or transient.  Once it has these fields, it tries to write out a number of types in a compact manner (primitive types, primitive wrapper classes, arrays of primitives, String, BigDecimal, Date, etc.).  

If it encounters a class that it does not have special handling for, it writes out some minimal metadata and then proceeds to reflectively write out its fields.

Getting started is as simple as selecting a data store and instantiating the Serializer:

    // create an in memory data store backing the persistent serializer
    InMemoryDataStore dataStore = new InMemoryDataStore();
    	
    // serializer itself that will persist and lookup metadata in the store
    Serializer serializer = new PersistedMetaDataSerializer(dataStore);

    // try serializing and deserializing a pojo
    byte[] payload = serializer.serialize(new SimplePojo());
    SimplePojo pojo = (SimplePojo)serializer.deserialize(payload);

Compact Serialized Representation
----
In order to keep the size of the serialized data small, the serializer separates the metadata from the actual content being serialized.  When creating a serializer, an implementation of a data store is supplied to it.  Using this data store, the serializer will create and store metadata about a class the first time it sees it.  This allows the serializer to serialize the fields in a specific order, but without that order needing to be maintained as the fields of the class change.

While deserializing, identifiers will be encountered in the data whenever an class without special handling is encountered.  The deserializer uses the data store to load up the appropriate metadata in order to deserialize the class.

Flexiblity
----
Each class will need a different strategy when it comes to handling version changes.  The persistent serializer is geared toward a use case where deserialization should succeed as much as possible, and while some data may not map perfectly to changed versions of the class, as many of the matching fields should be set as possible.

In order to accomplish this, the persisted metadata is keyed by a SHA-1 hash based on the declared fields of the class -- taking into account both their name and type.  The persisted metadata also contains the field names and types.  The deserializer looks up the appropriate metadata based on the version of the class that was originally serialized.  As fields are encountered, their original name and type are looked up.

Now that the deserializer has the original name and type of the field, it can search the current version of the class for a match.  If it finds one, it can set the value, regardless of the order of the fields in the new class.

Persistence
----
The DataStore used by the serializer contains metadata of classes that have been serialized.  The backend to the datastore needs to last as long as the serialized data itself is kept around -- without that guarantee we would not be able to lookup metadata that is necessary for deserialization.

In addition to storing metadata, the data store provides a compact identifier that is used in place of a key in the actual serialized data.  This is for the most part transparent to the data store, but in order for it to be accomplished the data store needs to provide a way of providing a counter that can be atomically incrementing across the scope of potential concurrent users of the data store.  In a single JVM, an AtomicLong works fine, but if the data store is shared across a cluster of application, some sort of shared counter is necessary.

A key for a class might look like _org.something.MyClass/c299b234b76777171d8b8a9d3ad48f6c2bdfea8e_ which is a concatenation of the class name and the SHA-1 over its fields.  The key that is used in its place for serialization is a _long_ that is mapped to the key in the data store.  So, during deserialization the long provides the mapping back to the key, which in turn is used to look up the metadata.

Since data associated with a key in the data store never changes, an aggressive cache can be put in front of it so that the only time the cache is not necessary is when a new class is encountered for the first time.

The repository contains an example *InMemoryDataStore*.  It stores data in a map, and uses an AtomicLong for the atomic counter.  It is great to try things out with, but you'll eventually want a data store with a more persistent backend like a database.  The data store interface is pretty simple so it should map to many persistance mechanisms.

Improvements for Custom Versioning
----
This approach of avoiding failures while deserializing is great, but there may be use cases where it is insufficient and custom handling of versioning may be required.  One idea is to offer an optional interface that classes can implement.  If this interface is implemented, a method will be called whenever a field is encountered in the serialized data that does not have a current match.  This would allow the majority of fields to be set automatically, and the versioning code within the object to concern itself only with changed fields.  

An alternative is to offer registration of conflict resolvers with the deserializer.  The conflict resolver would be indicate classes it is interested in, and it would be notified during deserialization of fields that fail to deserialize -- with the option of mapping that data appropriately.
