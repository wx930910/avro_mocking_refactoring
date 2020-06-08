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

public class TestCircularReferencesWithMock {
	@Rule
	public TemporaryFolder temp = new TemporaryFolder();

	/**
	 * Mocking First Class Reference Since it contains final fields, it may mock
	 * by combining EasyMock with PowerMock, will catch up it later.
	 */
	public static class MockLogicalType {
		public LogicalType MockedLogicalType;
		private static final String REFERENCE = "reference";
		private static final String REF_FIELD_NAME = "ref-field-name";

		private final String refFieldName;

		public MockLogicalType(String refFieldName) {
			MockedLogicalType = EasyMock.partialMockBuilder(LogicalType.class).withConstructor(String.class).withArgs(REFERENCE).createMock();
			this.refFieldName = refFieldName;
			MockAddToSchema();
		}

		public MockLogicalType(Schema schema) {
			MockedLogicalType = EasyMock.partialMockBuilder(LogicalType.class).withConstructor(String.class).withArgs(REFERENCE).createMock();
			this.refFieldName = schema.getProp(REF_FIELD_NAME);
			MockAddToSchema();
		}

		private void MockAddToSchema() {
			Capture<Schema> addToSchemaMethodArg1 = EasyMock.newCapture();
			EasyMock.expect(MockedLogicalType.addToSchema(EasyMock.capture(addToSchemaMethodArg1))).andAnswer(new IAnswer<Schema>() {
				@Override
				public Schema answer() {
					MockedLogicalType.addToSchema(addToSchemaMethodArg1.getValue());
					addToSchemaMethodArg1.getValue().addProp(REF_FIELD_NAME, refFieldName);
					return addToSchemaMethodArg1.getValue();
				}
			}).anyTimes();
		}

		private void MockGetName() {
			EasyMock.expect(MockedLogicalType.getName()).andReturn(REFERENCE);
		}

		private String getRefFieldName() {
			return this.refFieldName;
		}

	}

	public static class Reference extends LogicalType {
		private static final String REFERENCE = "reference";
		private static final String REF_FIELD_NAME = "ref-field-name";

		private final String refFieldName;

		public Reference(String refFieldName) {
			super(REFERENCE);
			this.refFieldName = refFieldName;
		}

		public Reference(Schema schema) {
			super(REFERENCE);
			this.refFieldName = schema.getProp(REF_FIELD_NAME);
		}

		@Override
		public Schema addToSchema(Schema schema) {
			super.addToSchema(schema);
			schema.addProp(REF_FIELD_NAME, refFieldName);
			return schema;
		}

		@Override
		public String getName() {
			return REFERENCE;
		}

		public String getRefFieldName() {
			return refFieldName;
		}

		@Override
		public void validate(Schema schema) {
			super.validate(schema);
			if (schema.getField(refFieldName) == null) {
				throw new IllegalArgumentException("Invalid field name for reference field: " + refFieldName);
			}
		}
	}

	public static class MockReferenceTypeFactory {
		public LogicalTypes.LogicalTypeFactory MockedReferenceTypeFactory;

		public MockReferenceTypeFactory() {
			this.MockedReferenceTypeFactory = EasyMock.niceMock(LogicalTypes.LogicalTypeFactory.class);
			MockFromSchema();
			MockGetTypeName();
		}

		private void MockFromSchema() {
			Capture<Schema> arg1 = EasyMock.newCapture();
			EasyMock.expect(this.MockedReferenceTypeFactory.fromSchema(EasyMock.capture(arg1))).andAnswer(new IAnswer<LogicalType>() {
				@Override
				public LogicalType answer() {
					return new Reference(arg1.getValue());
				}
			});
		}

		private void MockGetTypeName() {
			EasyMock.expect(this.MockedReferenceTypeFactory.getTypeName()).andReturn(Reference.REFERENCE);
		}

	}

	public static class Referenceable extends LogicalType {
		private static final String REFERENCEABLE = "referenceable";
		private static final String ID_FIELD_NAME = "id-field-name";

		private final String idFieldName;

		public Referenceable(String idFieldName) {
			super(REFERENCEABLE);
			this.idFieldName = idFieldName;
		}

		public Referenceable(Schema schema) {
			super(REFERENCEABLE);
			this.idFieldName = schema.getProp(ID_FIELD_NAME);
		}

		@Override
		public Schema addToSchema(Schema schema) {
			super.addToSchema(schema);
			schema.addProp(ID_FIELD_NAME, idFieldName);
			return schema;
		}

		@Override
		public String getName() {
			return REFERENCEABLE;
		}

		public String getIdFieldName() {
			return idFieldName;
		}

		@Override
		public void validate(Schema schema) {
			super.validate(schema);
			Schema.Field idField = schema.getField(idFieldName);
			if (idField == null || idField.schema().getType() != Schema.Type.LONG) {
				throw new IllegalArgumentException("Invalid ID field: " + idFieldName + ": " + idField);
			}
		}
	}

	public static class MockReferenceableTypeFactory {
		public LogicalTypes.LogicalTypeFactory MockedReferenceableTypeFactory;

		public MockReferenceableTypeFactory() {
			this.MockedReferenceableTypeFactory = EasyMock.niceMock(LogicalTypes.LogicalTypeFactory.class);
			MockFromSchema();
			MockGetTypeName();
		}

		private void MockFromSchema() {
			Capture<Schema> arg1 = EasyMock.newCapture();
			EasyMock.expect(this.MockedReferenceableTypeFactory.fromSchema(EasyMock.capture(arg1))).andAnswer(new IAnswer<LogicalType>() {
				@Override
				public LogicalType answer() {
					return new Referenceable(arg1.getValue());
				}
			});
		}

		private void MockGetTypeName() {
			EasyMock.expect(this.MockedReferenceableTypeFactory.getTypeName()).andReturn(Referenceable.REFERENCEABLE);
		}

	}

	@BeforeClass
	public static void addReferenceTypes() {
		LogicalTypes.register(Referenceable.REFERENCEABLE, new MockReferenceableTypeFactory().MockedReferenceableTypeFactory);
		LogicalTypes.register(Reference.REFERENCE, new MockReferenceTypeFactory().MockedReferenceTypeFactory);
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
				this.MockedReferenceableTracker = EasyMock.partialMockBuilder(Conversion.class)
						.addMockedMethod("toRecord", Object.class, Schema.class, LogicalType.class)
						.addMockedMethod("fromRecord", IndexedRecord.class, Schema.class, LogicalType.class).createMock();
				mockGetConvertedType();
				mockGetLogicalTypeName();
				mockToRecord();
				mockFromRecord();
				EasyMock.replay(this.MockedReferenceableTracker);
			}

			private void mockGetConvertedType() {
				EasyMock.expect(this.MockedReferenceableTracker.getConvertedType()).andReturn((Class) Record.class).anyTimes();
			}

			private void mockGetLogicalTypeName() {
				EasyMock.expect(this.MockedReferenceableTracker.getLogicalTypeName()).andReturn(Referenceable.REFERENCEABLE).anyTimes();
			}

			private void mockFromRecord() {
				Capture<IndexedRecord> value = EasyMock.newCapture();
				Capture<Schema> schema = EasyMock.newCapture();
				Capture<LogicalType> type = EasyMock.newCapture();
				EasyMock.expect(this.MockedReferenceableTracker.fromRecord(EasyMock.capture(value), EasyMock.capture(schema), EasyMock.capture(type)))
						.andAnswer(new IAnswer<IndexedRecord>() {
							@Override
							public IndexedRecord answer() {
								// read side
								long id = getId(value.getValue(), schema.getValue());

								// keep track of this for later references
								references.put(id, value.getValue());

								// call any callbacks waiting to resolve this id
								List<Callback> callbacks = callbacksById.get(id);
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
				EasyMock.expect(this.MockedReferenceableTracker.toRecord(EasyMock.capture(value), EasyMock.capture(schema),
						EasyMock.anyObject(LogicalType.class))).andAnswer(new IAnswer<IndexedRecord>() {
							@Override
							public IndexedRecord answer() {
								long id = getId(value.getValue(), schema.getValue());

								// keep track of this for later references
								// references.put(id, value);
								ids.put(value.getValue(), id);

								return value.getValue();
							}
						}).anyTimes();
			}

			private long getId(IndexedRecord referenceable, Schema schema) {
				Referenceable info = (Referenceable) schema.getLogicalType();
				int idField = schema.getField(info.getIdFieldName()).pos();
				return (Long) referenceable.get(idField);
			}

		}

		public class MockReferenceHandler {
			Conversion<IndexedRecord> MockedReferenceHandler;

			public MockReferenceHandler() {
				this.MockedReferenceHandler = EasyMock.partialMockBuilder(Conversion.class)
						.addMockedMethod("fromRecord", IndexedRecord.class, Schema.class, LogicalType.class)
						.addMockedMethod("toRecord", Object.class, Schema.class, LogicalType.class).createMock();
				mockGetConvertedType();
				mockGetLogicalTypeName();
				mockFromRecord();
				mockToRecord();
				EasyMock.replay(this.MockedReferenceHandler);
			}

			private void mockGetConvertedType() {
				EasyMock.expect(this.MockedReferenceHandler.getConvertedType()).andReturn((Class) Record.class).anyTimes();
			}

			private void mockGetLogicalTypeName() {
				EasyMock.expect(this.MockedReferenceHandler.getLogicalTypeName()).andReturn(Reference.REFERENCE).anyTimes();
			}

			private void mockFromRecord() {
				final Capture<IndexedRecord> record = EasyMock.newCapture();
				Capture<Schema> schema = EasyMock.newCapture();
				Capture<LogicalType> type = EasyMock.newCapture();
				EasyMock.expect(this.MockedReferenceHandler.fromRecord(EasyMock.capture(record), EasyMock.capture(schema), EasyMock.capture(type)))
						.andAnswer(new IAnswer<IndexedRecord>() {
							@Override
							public IndexedRecord answer() {
								final Schema.Field refField = schema.getValue().getField(((Reference) type.getValue()).getRefFieldName());

								Long id = (Long) record.getValue().get(refField.pos());
								if (id != null) {
									if (references.containsKey(id)) {
										record.getValue().put(refField.pos(), references.get(id));

									} else {
										List<Callback> callbacks = callbacksById.computeIfAbsent(id, k -> new ArrayList<>());
										// add a callback to resolve this
										// reference when the id
										// is available
										callbacks.add(referenceable -> record.getValue().put(refField.pos(), referenceable));
									}
								}

								return record.getValue();
							}
						}).anyTimes();
			}

			private void mockToRecord() {
				Capture<IndexedRecord> record = EasyMock.newCapture();
				Capture<Schema> schema = EasyMock.newCapture();
				Capture<LogicalType> type = EasyMock.newCapture();
				EasyMock.expect(this.MockedReferenceHandler.toRecord(EasyMock.capture(record), EasyMock.capture(schema), EasyMock.capture(type)))
						.andAnswer(new IAnswer<IndexedRecord>() {
							@Override
							public IndexedRecord answer() {
								// write side: replace a referenced field with
								// its id
								Schema.Field refField = schema.getValue().getField(((Reference) type.getValue()).getRefFieldName());
								IndexedRecord referenced = (IndexedRecord) record.getValue().get(refField.pos());
								if (referenced == null) {
									return record.getValue();
								}

								// hijack the field to return the id instead of
								// the ref
								return new HijackingIndexedRecord(record.getValue(), refField.pos(), ids.get(referenced));
							}
						}).anyTimes();
			}

		}

		private static class HijackingIndexedRecord implements IndexedRecord {
			private final IndexedRecord wrapped;
			private final int index;
			private final Object data;

			public HijackingIndexedRecord(IndexedRecord wrapped, int index, Object data) {
				this.wrapped = wrapped;
				this.index = index;
				this.data = data;
			}

			@Override
			public void put(int i, Object v) {
				throw new RuntimeException("[BUG] This is a read-only class.");
			}

			@Override
			public Object get(int i) {
				if (i == index) {
					return data;
				}
				return wrapped.get(i);
			}

			@Override
			public Schema getSchema() {
				return wrapped.getSchema();
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

		Schema parentRefSchema = Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.LONG), parentSchema);
		Reference parentRef = new Reference("parent");

		List<Schema.Field> childFields = new ArrayList<>();
		childFields.add(new Schema.Field("c", Schema.create(Schema.Type.STRING)));
		childFields.add(new Schema.Field("parent", parentRefSchema));
		Schema childSchema = parentRef.addToSchema(Schema.createRecord("Child", null, null, false, childFields));

		List<Schema.Field> parentFields = new ArrayList<>();
		parentFields.add(new Schema.Field("id", Schema.create(Schema.Type.LONG)));
		parentFields.add(new Schema.Field("p", Schema.create(Schema.Type.STRING)));
		parentFields.add(new Schema.Field("child", childSchema));
		parentSchema.setFields(parentFields);
		Referenceable idRef = new Referenceable("id");

		Schema schema = idRef.addToSchema(parentSchema);

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
		Assert.assertEquals("Should correctly read back the parent id", 1L, actual.get("id"));
		Assert.assertEquals("Should correctly read back the parent data", new Utf8("parent data!"), actual.get("p"));

		Record actualChild = (Record) actual.get("child");
		Assert.assertEquals("Should correctly read back the child data", new Utf8("child data!"), actualChild.get("c"));
		Object childParent = actualChild.get("parent");
		Assert.assertTrue("Should have a parent Record object", childParent instanceof Record);

		Record childParentRecord = (Record) actualChild.get("parent");
		Assert.assertEquals("Should have the right parent id", 1L, childParentRecord.get("id"));
		Assert.assertEquals("Should have the right parent data", new Utf8("parent data!"), childParentRecord.get("p"));
	}

	private <D> List<D> read(GenericData model, Schema schema, File file) throws IOException {
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
	private <D> File write(GenericData model, Schema schema, D... data) throws IOException {
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
