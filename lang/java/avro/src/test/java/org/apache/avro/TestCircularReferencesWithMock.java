package org.apache.avro;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.util.Utf8;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

public class TestCircularReferencesWithMock {
	@Rule
	public TemporaryFolder temp = new TemporaryFolder();
	public static Map<LogicalType, MockReference> MockReferenceMap = new HashMap<>();
	public static Map<LogicalType, MockReferenceable> MockRefereneableMap = new HashMap<>();

	/**
	 * Mocking First Class Reference Since it contains final fields, it may mock
	 * by combining EasyMock with PowerMock, will catch up it later.
	 */
	public static class MockReference {
		public LogicalType MockedLogicalType;
		private static final String REFERENCE = "reference";
		private static final String REF_FIELD_NAME = "ref-field-name";

		private final String refFieldName;

		public MockReference(String refFieldName) {
			MockedLogicalType = Mockito.mock(LogicalType.class,
					Mockito.withSettings()
							.defaultAnswer(Mockito.CALLS_REAL_METHODS)
							.useConstructor(REFERENCE));
			this.refFieldName = refFieldName;
			MockAddToSchema();
			MockGetName();
			mockValidate();
			MockReferenceMap.put(MockedLogicalType, this);
		}

		public MockReference(Schema schema) {
			MockedLogicalType = Mockito.mock(LogicalType.class,
					Mockito.withSettings()
							.defaultAnswer(Mockito.CALLS_REAL_METHODS)
							.useConstructor(REFERENCE));
			this.refFieldName = schema.getProp(REF_FIELD_NAME);
			MockAddToSchema();
			MockGetName();
			mockValidate();
		}

		private void MockAddToSchema() {
			Mockito.doAnswer(invocation -> {
				Schema schema = invocation.getArgument(0);
				invocation.callRealMethod();
				schema.addProp(REF_FIELD_NAME, refFieldName);
				return schema;
			}).when(this.MockedLogicalType)
					.addToSchema(Mockito.any(Schema.class));
		}

		private void MockGetName() {
			Mockito.doReturn(REFERENCE).when(this.MockedLogicalType).getName();
		}

		public String getRefFieldName() {
			return this.refFieldName;
		}

		private void mockValidate() {
			Mockito.doAnswer(invocation -> {
				Schema schema = invocation.getArgument(0);
				invocation.callRealMethod();
				if (schema.getField(refFieldName) == null) {
					throw new IllegalArgumentException(
							"Invalid field name for reference field: "
									+ refFieldName);
				}
				return null;
			}).when(this.MockedLogicalType).validate(Mockito.any(Schema.class));
		}

	}

	public static class MockReferenceTypeFactory {
		public LogicalTypes.LogicalTypeFactory MockedReferenceTypeFactory;

		public MockReferenceTypeFactory() {
			this.MockedReferenceTypeFactory = EasyMock
					.niceMock(LogicalTypes.LogicalTypeFactory.class);
			MockFromSchema();
			MockGetTypeName();
		}

		private void MockFromSchema() {
			Capture<Schema> arg1 = EasyMock.newCapture();
			EasyMock.expect(this.MockedReferenceTypeFactory
					.fromSchema(EasyMock.capture(arg1)))
					.andAnswer(new IAnswer<LogicalType>() {
						@Override
						public LogicalType answer() {
							return new MockReference(
									arg1.getValue()).MockedLogicalType;
						}
					});
		}

		private void MockGetTypeName() {
			EasyMock.expect(this.MockedReferenceTypeFactory.getTypeName())
					.andReturn(MockReference.REFERENCE);
		}

	}

	public static class MockReferenceable {
		public LogicalType instance;
		private static final String REFERENCEABLE = "referenceable";
		private static final String ID_FIELD_NAME = "id-field-name";

		private final String idFieldName;

		public MockReferenceable(String idFieldName) {
			this.instance = Mockito.mock(LogicalType.class,
					Mockito.withSettings()
							.defaultAnswer(Mockito.CALLS_REAL_METHODS)
							.useConstructor(REFERENCEABLE));
			this.idFieldName = idFieldName;
			MockAddToSchema();
			MockGetName();
			mockValidate();
			MockRefereneableMap.put(instance, this);
		}

		public MockReferenceable(Schema schema) {
			this.instance = Mockito.mock(LogicalType.class,
					Mockito.withSettings()
							.defaultAnswer(Mockito.CALLS_REAL_METHODS)
							.useConstructor(REFERENCEABLE));
			this.idFieldName = schema.getProp(ID_FIELD_NAME);
			MockAddToSchema();
			MockGetName();
			mockValidate();
			MockRefereneableMap.put(instance, this);
		}

		private void MockAddToSchema() {
			Mockito.doAnswer(invocation -> {
				Schema schema = invocation.getArgument(0);
				invocation.callRealMethod();
				schema.addProp(ID_FIELD_NAME, idFieldName);
				return schema;
			}).when(this.instance).addToSchema(Mockito.any(Schema.class));
		}

		private void MockGetName() {
			Mockito.doReturn(REFERENCEABLE).when(this.instance).getName();
		}

		public String getIdFieldName() {
			return idFieldName;
		}

		private void mockValidate() {
			Mockito.doAnswer(invocation -> {
				Schema schema = invocation.getArgument(0);
				invocation.callRealMethod();
				Schema.Field idField = schema.getField(idFieldName);
				if (idField == null
						|| idField.schema().getType() != Schema.Type.LONG) {
					throw new IllegalArgumentException("Invalid ID field: "
							+ idFieldName + ": " + idField);
				}
				return null;
			}).when(this.instance).validate(Mockito.any(Schema.class));
		}

	}

	public static class MockReferenceableTypeFactory {
		public LogicalTypes.LogicalTypeFactory MockedReferenceableTypeFactory;

		public MockReferenceableTypeFactory() {
			this.MockedReferenceableTypeFactory = EasyMock
					.niceMock(LogicalTypes.LogicalTypeFactory.class);
			MockFromSchema();
			MockGetTypeName();
		}

		private void MockFromSchema() {
			Capture<Schema> arg1 = EasyMock.newCapture();
			EasyMock.expect(this.MockedReferenceableTypeFactory
					.fromSchema(EasyMock.capture(arg1)))
					.andAnswer(new IAnswer<LogicalType>() {
						@Override
						public LogicalType answer() {
							return new MockReferenceable(
									arg1.getValue()).instance;
						}
					});
		}

		private void MockGetTypeName() {
			EasyMock.expect(this.MockedReferenceableTypeFactory.getTypeName())
					.andReturn(MockReferenceable.REFERENCEABLE);
		}

	}

	@BeforeClass
	public static void addReferenceTypes() {
		LogicalTypes.register(MockReferenceable.REFERENCEABLE,
				new MockReferenceableTypeFactory().MockedReferenceableTypeFactory);
		LogicalTypes.register(MockReference.REFERENCE,
				new MockReferenceTypeFactory().MockedReferenceTypeFactory);
	}

	public static class ReferenceManager {
		private interface Callback {
			void set(Object referenceable);
		}

		private final Map<Long, Object> references = new HashMap<>();
		private final Map<Object, Long> ids = new IdentityHashMap<>();
		private final Map<Long, List<Callback>> callbacksById = new HashMap<>();
		private final Conversion<IndexedRecord> tracker = new MockReferenceableTracker().MockedReferenceableTracker;
		private final Conversion<IndexedRecord> handler = new MockReferenceHandler().MockedReferenceHandler;

		public Conversion<IndexedRecord> getTracker() {
			return tracker;
		}

		public Conversion<IndexedRecord> getHandler() {
			return handler;
		}

		/**
		 * Mock
		 * {@link TestCircularReferences#ReferenceManager#ReferenceableTracker}
		 * 
		 * @author Twilight
		 *
		 */

		public class MockReferenceableTracker {
			Conversion<IndexedRecord> MockedReferenceableTracker;

			public MockReferenceableTracker() {
				this.MockedReferenceableTracker = EasyMock
						.partialMockBuilder(Conversion.class)
						.addMockedMethod("toRecord", Object.class, Schema.class,
								LogicalType.class)
						.addMockedMethod("fromRecord", IndexedRecord.class,
								Schema.class, LogicalType.class)
						.createMock();
				mockGetConvertedType();
				mockGetLogicalTypeName();
				mockToRecord();
				mockFromRecord();
				EasyMock.replay(this.MockedReferenceableTracker);
			}

			private void mockGetConvertedType() {
				EasyMock.expect(
						this.MockedReferenceableTracker.getConvertedType())
						.andReturn((Class) Record.class).anyTimes();
			}

			private void mockGetLogicalTypeName() {
				EasyMock.expect(
						this.MockedReferenceableTracker.getLogicalTypeName())
						.andReturn(MockReferenceable.REFERENCEABLE).anyTimes();
			}

			private void mockFromRecord() {
				Capture<IndexedRecord> value = EasyMock.newCapture();
				Capture<Schema> schema = EasyMock.newCapture();
				Capture<LogicalType> type = EasyMock.newCapture();
				EasyMock.expect(this.MockedReferenceableTracker.fromRecord(
						EasyMock.capture(value), EasyMock.capture(schema),
						EasyMock.capture(type)))
						.andAnswer(new IAnswer<IndexedRecord>() {
							@Override
							public IndexedRecord answer() {
								// read side
								long id = getId(value.getValue(),
										schema.getValue());

								// keep track of this for later references
								references.put(id, value.getValue());

								// call any callbacks waiting to resolve this id
								List<Callback> callbacks = callbacksById
										.get(id);
								for (Callback callback : callbacks) {
									callback.set(value.getValue());
								}

								return value.getValue();
							}
						}).anyTimes();
			}

			private void mockToRecord() {
				Capture<IndexedRecord> value = EasyMock.newCapture();
				Capture<Schema> schema = EasyMock.newCapture();
				EasyMock.expect(this.MockedReferenceableTracker.toRecord(
						EasyMock.capture(value), EasyMock.capture(schema),
						EasyMock.anyObject(LogicalType.class)))
						.andAnswer(new IAnswer<IndexedRecord>() {
							@Override
							public IndexedRecord answer() {
								long id = getId(value.getValue(),
										schema.getValue());

								// keep track of this for later references
								// references.put(id, value);
								ids.put(value.getValue(), id);

								return value.getValue();
							}
						}).anyTimes();
			}

			private long getId(IndexedRecord referenceable, Schema schema) {
				LogicalType info = schema.getLogicalType();
				int idField = schema
						.getField(
								MockRefereneableMap.get(info).getIdFieldName())
						.pos();
				return (Long) referenceable.get(idField);
			}

		}

		public class MockReferenceHandler {
			Conversion<IndexedRecord> MockedReferenceHandler;

			public MockReferenceHandler() {
				this.MockedReferenceHandler = EasyMock
						.partialMockBuilder(Conversion.class)
						.addMockedMethod("fromRecord", IndexedRecord.class,
								Schema.class, LogicalType.class)
						.addMockedMethod("toRecord", Object.class, Schema.class,
								LogicalType.class)
						.createMock();
				mockGetConvertedType();
				mockGetLogicalTypeName();
				mockFromRecord();
				mockToRecord();
				EasyMock.replay(this.MockedReferenceHandler);
			}

			private void mockGetConvertedType() {
				EasyMock.expect(this.MockedReferenceHandler.getConvertedType())
						.andReturn((Class) Record.class).anyTimes();
			}

			private void mockGetLogicalTypeName() {
				EasyMock.expect(
						this.MockedReferenceHandler.getLogicalTypeName())
						.andReturn(MockReference.REFERENCE).anyTimes();
			}

			private void mockFromRecord() {
				EasyMock.expect(this.MockedReferenceHandler.fromRecord(
						EasyMock.anyObject(IndexedRecord.class),
						EasyMock.anyObject(Schema.class),
						EasyMock.anyObject(LogicalType.class)))
						.andAnswer(new IAnswer<IndexedRecord>() {
							@Override
							public IndexedRecord answer() {
								final IndexedRecord record = EasyMock
										.getCurrentArgument(0);
								Schema schema = EasyMock.getCurrentArgument(1);
								LogicalType type = EasyMock
										.getCurrentArgument(2);
								final Schema.Field refField = schema
										.getField(MockReferenceMap.get(type)
												.getRefFieldName());

								Long id = (Long) record.get(refField.pos());
								if (id != null) {
									if (references.containsKey(id)) {
										record.put(refField.pos(),
												references.get(id));

									} else {
										List<Callback> callbacks = callbacksById
												.computeIfAbsent(id,
														k -> new ArrayList<>());
										// add a callback to resolve this
										// reference when the id
										// is available
										callbacks.add(referenceable -> record
												.put(refField.pos(),
														referenceable));
									}
								}

								return record;
							}
						}).anyTimes();
			}

			private void mockToRecord() {

				EasyMock.expect(this.MockedReferenceHandler.toRecord(
						EasyMock.anyObject(IndexedRecord.class),
						EasyMock.anyObject(Schema.class),
						EasyMock.anyObject(LogicalType.class)))
						.andAnswer(new IAnswer<IndexedRecord>() {
							@Override
							public IndexedRecord answer() {
								IndexedRecord record = EasyMock
										.getCurrentArgument(0);
								Schema schema = EasyMock.getCurrentArgument(1);
								LogicalType type = EasyMock
										.getCurrentArgument(2);
								// write side: replace a referenced field with
								// its id
								Schema.Field refField = schema
										.getField(MockReferenceMap.get(type)
												.getRefFieldName());
								IndexedRecord referenced = (IndexedRecord) record
										.get(refField.pos());
								if (referenced == null) {
									return record;
								}

								// hijack the field to return the id instead of
								// the ref
								return new MockHijackingIndexedRecord(record,
										refField.pos(),
										ids.get(referenced)).MockedHijackingIndexedRecord;
							}
						}).anyTimes();
			}

		}

		private static class MockHijackingIndexedRecord {
			public IndexedRecord MockedHijackingIndexedRecord;
			private final IndexedRecord wrapped;
			private final int index;
			private final Object data;

			public MockHijackingIndexedRecord(IndexedRecord wrapped, int index,
					Object data) {
				this.MockedHijackingIndexedRecord = EasyMock
						.niceMock(IndexedRecord.class);
				this.wrapped = wrapped;
				this.index = index;
				this.data = data;
				mockPut();
				mockGet();
				mockGetSchema();
				EasyMock.replay(this.MockedHijackingIndexedRecord);
			}

			private void mockPut() {
				this.MockedHijackingIndexedRecord.put(EasyMock.anyInt(),
						EasyMock.anyObject());
				EasyMock.expectLastCall().andAnswer(() -> {
					throw new RuntimeException(
							"[BUG] This is a read-only class.");
				}).anyTimes();
			}

			private void mockGet() {
				EasyMock.expect(this.MockedHijackingIndexedRecord
						.get(EasyMock.anyInt())).andAnswer(() -> {
							int i = EasyMock.getCurrentArgument(0);
							if (i == index) {
								return data;
							}
							return wrapped.get(i);
						}).anyTimes();
			}

			private void mockGetSchema() {
				EasyMock.expect(this.MockedHijackingIndexedRecord.getSchema())
						.andAnswer(() -> {
							return wrapped.getSchema();
						}).anyTimes();
			}

		}

	}

	@Test
	public void test() throws IOException {
		ReferenceManager manager = new ReferenceManager();
		GenericData model = new GenericData();
		model.addLogicalTypeConversion(manager.getTracker());
		model.addLogicalTypeConversion(manager.getHandler());

		Schema parentSchema = Schema.createRecord("Parent", null, null, false);

		Schema parentRefSchema = Schema.createUnion(
				Schema.create(Schema.Type.NULL),
				Schema.create(Schema.Type.LONG), parentSchema);
		MockReference parentRef = new MockReference("parent");

		List<Schema.Field> childFields = new ArrayList<>();
		childFields
				.add(new Schema.Field("c", Schema.create(Schema.Type.STRING)));
		childFields.add(new Schema.Field("parent", parentRefSchema));
		Schema childSchema = parentRef.MockedLogicalType.addToSchema(
				Schema.createRecord("Child", null, null, false, childFields));

		List<Schema.Field> parentFields = new ArrayList<>();
		parentFields
				.add(new Schema.Field("id", Schema.create(Schema.Type.LONG)));
		parentFields
				.add(new Schema.Field("p", Schema.create(Schema.Type.STRING)));
		parentFields.add(new Schema.Field("child", childSchema));
		parentSchema.setFields(parentFields);
		MockReferenceable idRef = new MockReferenceable("id");

		Schema schema = idRef.instance.addToSchema(parentSchema);

		System.out.println("Schema: " + schema.toString(true));

		Record parent = new Record(schema);
		parent.put("id", 1L);
		parent.put("p", "parent data!");

		Record child = new Record(childSchema);
		child.put("c", "child data!");
		child.put("parent", parent);

		parent.put("child", child);

		// serialization round trip
		File data = write(model, schema, parent);
		List<Record> records = read(model, schema, data);

		Record actual = records.get(0);

		// because the record is a recursive structure, equals won't work
		Assert.assertEquals("Should correctly read back the parent id", 1L,
				actual.get("id"));
		Assert.assertEquals("Should correctly read back the parent data",
				new Utf8("parent data!"), actual.get("p"));

		Record actualChild = (Record) actual.get("child");
		Assert.assertEquals("Should correctly read back the child data",
				new Utf8("child data!"), actualChild.get("c"));
		Object childParent = actualChild.get("parent");
		Assert.assertTrue("Should have a parent Record object",
				childParent instanceof Record);

		Record childParentRecord = (Record) actualChild.get("parent");
		Assert.assertEquals("Should have the right parent id", 1L,
				childParentRecord.get("id"));
		Assert.assertEquals("Should have the right parent data",
				new Utf8("parent data!"), childParentRecord.get("p"));
	}

	private <D> List<D> read(GenericData model, Schema schema, File file)
			throws IOException {
		DatumReader<D> reader = newReader(model, schema);
		List<D> data = new ArrayList<>();

		try (FileReader<D> fileReader = new DataFileReader<>(file, reader)) {
			for (D datum : fileReader) {
				data.add(datum);
			}
		}

		return data;
	}

	@SuppressWarnings("unchecked")
	private <D> DatumReader<D> newReader(GenericData model, Schema schema) {
		return model.createDatumReader(schema);
	}

	@SuppressWarnings("unchecked")
	private <D> File write(GenericData model, Schema schema, D... data)
			throws IOException {
		File file = temp.newFile();
		DatumWriter<D> writer = model.createDatumWriter(schema);

		try (DataFileWriter<D> fileWriter = new DataFileWriter<>(writer)) {
			fileWriter.create(schema, file);
			for (D datum : data) {
				fileWriter.append(datum);
			}
		}

		return file;
	}
}
