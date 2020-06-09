package org.apache.avro.specific;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

public class TestSpecificDataWithMock {
	private Class<?> intClass;
	private Class<?> integerClass;

	@Before
	public void setUp() {
		Schema intSchema = Schema.create(Type.INT);
		intClass = SpecificData.get().getClass(intSchema);
		Schema nullSchema = Schema.create(Type.NULL);
		Schema nullIntUnionSchema = Schema.createUnion(Arrays.asList(nullSchema, intSchema));
		integerClass = SpecificData.get().getClass(nullIntUnionSchema);
	}

	@Test
	public void testClassTypes() {
		assertTrue(intClass.isPrimitive());
		assertFalse(integerClass.isPrimitive());
	}

	@Test
	public void testPrimitiveParam() throws Exception {
		assertNotNull(Reflection.class.getMethod("primitive", intClass));
	}

	@Test(expected = NoSuchMethodException.class)
	public void testPrimitiveParamError() throws Exception {
		Reflection.class.getMethod("primitiveWrapper", intClass);
	}

	@Test
	public void testPrimitiveWrapperParam() throws Exception {
		assertNotNull(Reflection.class.getMethod("primitiveWrapper", integerClass));
	}

	@Test(expected = NoSuchMethodException.class)
	public void testPrimitiveWrapperParamError() throws Exception {
		Reflection.class.getMethod("primitive", integerClass);
	}

	static class Reflection {
		public void primitive(int i) {
		}

		public void primitiveWrapper(Integer i) {
		}
	}

	public static class MockTestRecord {
		public SpecificRecordBase MockedTestRecord;
		private static final Schema SCHEMA = Schema.createRecord("MockTestRecord", null, null, false);
		static {
			List<Field> fields = new ArrayList<>();
			fields.add(new Field("x", Schema.create(Type.INT), null, null));
			Schema stringSchema = Schema.create(Type.STRING);
			GenericData.setStringType(stringSchema, GenericData.StringType.String);
			fields.add(new Field("y", stringSchema, null, null));
			SCHEMA.setFields(fields);
		}
		private int x;
		private String y;

		public MockTestRecord() {
			this.MockedTestRecord = EasyMock.partialMockBuilder(SpecificRecordBase.class).createMock();
			mockPut();
			mockGet();
			mockGetSchema();
			EasyMock.replay(this.MockedTestRecord);
		}

		private void mockPut() {
			this.MockedTestRecord.put(EasyMock.anyInt(), EasyMock.anyObject());
			EasyMock.expectLastCall().andAnswer(() -> {
				int i = EasyMock.getCurrentArgument(0);
				Object v = EasyMock.getCurrentArgument(1);
				switch (i) {
				case 0:
					x = (Integer) v;
					break;
				case 1:
					y = (String) v;
					break;
				default:
					throw new RuntimeException();
				}
				return null;
			}).anyTimes();
		}

		private void mockGet() {
			this.MockedTestRecord.get(EasyMock.anyInt());
			EasyMock.expectLastCall().andAnswer(() -> {
				int i = EasyMock.getCurrentArgument(0);
				switch (i) {
				case 0:
					return x;
				case 1:
					return y;
				}
				throw new RuntimeException();
			}).anyTimes();
		}

		private void mockGetSchema() {
			EasyMock.expect(this.MockedTestRecord.getSchema()).andReturn(SCHEMA).anyTimes();
		}

	}

	@Test
	public void testSpecificRecordBase() {
		final MockTestRecord record = new MockTestRecord();
		record.MockedTestRecord.put("x", 1);
		record.MockedTestRecord.put("y", "str");
		assertEquals(1, record.MockedTestRecord.get("x"));
		assertEquals("str", record.MockedTestRecord.get("y"));
	}

	@Test
	public void testExternalizeable() throws Exception {
		final SpecificRecordBase before = new MockTestRecord().MockedTestRecord;
		before.put("x", 1);
		before.put("y", "str");
		ByteArrayOutputStream bytes = new ByteArrayOutputStream();
		ObjectOutputStream out = new ObjectOutputStream(bytes);
		out.writeObject(before);
		out.close();

		ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()));
		SpecificRecordBase after = (SpecificRecordBase) in.readObject();

		assertEquals(before, after);
	}

	/** Tests that non Stringable datum are rejected by specific writers. */
	@Test
	public void testNonStringable() throws Exception {
		final Schema string = Schema.create(Type.STRING);
		final ByteArrayOutputStream baos = new ByteArrayOutputStream();
		final Encoder encoder = EncoderFactory.get().directBinaryEncoder(baos, null);
		final DatumWriter<Object> writer = new SpecificDatumWriter<>(string);
		try {
			writer.write(new Object(), encoder);
			fail("Non stringable object should be rejected.");
		} catch (ClassCastException cce) {
			// Expected error
		}
	}
}
