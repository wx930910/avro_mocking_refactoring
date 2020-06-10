package org.apache.avro.generic;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.UnresolvedUnionException;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.util.Utf8;
import org.easymock.EasyMock;
import org.junit.Test;

public class TestGenericDatumWriterWithMock {
	@Test
	public void testUnionUnresolvedExceptionExplicitWhichField() throws IOException {
		Schema s = schemaWithExplicitNullDefault();
		GenericRecord r = new GenericData.Record(s);
		r.put("f", 100);
		ByteArrayOutputStream bao = new ByteArrayOutputStream();
		EncoderFactory.get().jsonEncoder(s, bao);
		try {
			new GenericDatumWriter<>(s).write(r, EncoderFactory.get().jsonEncoder(s, bao));
			fail();
		} catch (final UnresolvedUnionException uue) {
			assertEquals("Not in union [\"null\",\"string\"]: 100 (field=f)", uue.getMessage());
		}
	}

	@Test
	public void testWrite() throws IOException {
		String json = "{\"type\": \"record\", \"name\": \"r\", \"fields\": [" + "{ \"name\": \"f1\", \"type\": \"long\" }" + "]}";
		Schema s = new Schema.Parser().parse(json);
		GenericRecord r = new GenericData.Record(s);
		r.put("f1", 100L);
		ByteArrayOutputStream bao = new ByteArrayOutputStream();
		GenericDatumWriter<GenericRecord> w = new GenericDatumWriter<>(s);
		Encoder e = EncoderFactory.get().jsonEncoder(s, bao);
		w.write(r, e);
		e.flush();

		Object o = new GenericDatumReader<GenericRecord>(s).read(null,
				DecoderFactory.get().jsonDecoder(s, new ByteArrayInputStream(bao.toByteArray())));
		assertEquals(r, o);
	}

	@Test
	public void testArrayConcurrentModification() throws Exception {
		String json = "{\"type\": \"array\", \"items\": \"int\" }";
		Schema s = new Schema.Parser().parse(json);
		final GenericArray<Integer> a = new GenericData.Array<>(1, s);
		ByteArrayOutputStream bao = new ByteArrayOutputStream();
		final GenericDatumWriter<GenericArray<Integer>> w = new GenericDatumWriter<>(s);

		CountDownLatch sizeWrittenSignal = new CountDownLatch(1);
		CountDownLatch eltAddedSignal = new CountDownLatch(1);

		final MockTestEncoder e = new MockTestEncoder(EncoderFactory.get().directBinaryEncoder(bao, null), sizeWrittenSignal, eltAddedSignal);

		// call write in another thread
		ExecutorService executor = Executors.newSingleThreadExecutor();
		Future<Void> result = executor.submit(() -> {
			w.write(a, e.MockedTestEncoder);
			return null;
		});
		sizeWrittenSignal.await();
		// size has been written so now add an element to the array
		a.add(7);
		// and signal for the element to be written
		eltAddedSignal.countDown();
		try {
			result.get();
			fail("Expected ConcurrentModificationException");
		} catch (ExecutionException ex) {
			assertTrue(ex.getCause() instanceof ConcurrentModificationException);
		}
	}

	@Test
	public void testMapConcurrentModification() throws Exception {
		String json = "{\"type\": \"map\", \"values\": \"int\" }";
		Schema s = new Schema.Parser().parse(json);
		final Map<String, Integer> m = new HashMap<>();
		ByteArrayOutputStream bao = new ByteArrayOutputStream();
		final GenericDatumWriter<Map<String, Integer>> w = new GenericDatumWriter<>(s);

		CountDownLatch sizeWrittenSignal = new CountDownLatch(1);
		CountDownLatch eltAddedSignal = new CountDownLatch(1);

		final MockTestEncoder e = new MockTestEncoder(EncoderFactory.get().directBinaryEncoder(bao, null), sizeWrittenSignal, eltAddedSignal);

		// call write in another thread
		ExecutorService executor = Executors.newSingleThreadExecutor();
		Future<Void> result = executor.submit(() -> {
			w.write(m, e.MockedTestEncoder);
			return null;
		});
		sizeWrittenSignal.await();
		// size has been written so now add an entry to the map
		m.put("a", 7);
		// and signal for the entry to be written
		eltAddedSignal.countDown();
		try {
			result.get();
			fail("Expected ConcurrentModificationException");
		} catch (ExecutionException ex) {
			assertTrue(ex.getCause() instanceof ConcurrentModificationException);
		}
	}

	@Test
	public void testAllowWritingPrimitives() throws IOException {
		Schema doubleType = Schema.create(Schema.Type.DOUBLE);
		Schema.Field field = new Schema.Field("double", doubleType);
		List<Schema.Field> fields = Collections.singletonList(field);
		Schema schema = Schema.createRecord("test", "doc", "", false, fields);

		GenericRecord record = new GenericData.Record(schema);
		record.put("double", 456.4);
		record.put("double", 100000L);
		record.put("double", 444);

		ByteArrayOutputStream bao = new ByteArrayOutputStream();
		GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
		Encoder encoder = EncoderFactory.get().jsonEncoder(schema, bao);

		writer.write(record, encoder);
	}

	static class MockTestEncoder {
		Encoder e;
		CountDownLatch sizeWrittenSignal;
		CountDownLatch eltAddedSignal;

		public Encoder MockedTestEncoder;

		public MockTestEncoder(Encoder encoder, CountDownLatch sizeWrittenSignal, CountDownLatch eltAddedSignal) {
			this.e = encoder;
			this.sizeWrittenSignal = sizeWrittenSignal;
			this.eltAddedSignal = eltAddedSignal;
			this.MockedTestEncoder = EasyMock.partialMockBuilder(Encoder.class).createMock();
			mockWriteArrayStart();
			mockWriteMapStart();
			mockFlush();
			mockWriteNull();
			mockWriteBoolean();
			mockWriteInt();
			mockWriteLong();
			mockWriteFloat();
			mockWriteDouble();
			mockWriteString();
			mockWriteBytes1();
			mockWriteBytes2();
			mockWriteFixed();
			mockWriteEnum();
			mockSetItemCount();
			mockStartItem();
			mockWriteArrayEnd();
			mockWriteMapEnd();
			mockWriteIndex();
			EasyMock.replay(this.MockedTestEncoder);
		}

		private void mockWriteArrayStart() {
			try {
				this.MockedTestEncoder.writeArrayStart();
				EasyMock.expectLastCall().andAnswer(() -> {
					e.writeArrayStart();
					sizeWrittenSignal.countDown();
					try {
						eltAddedSignal.await();
					} catch (InterruptedException e) {
						// ignore
					}
					return null;
				}).andThrow(new IOException()).anyTimes();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		private void mockWriteMapStart() {
			try {
				this.MockedTestEncoder.writeMapStart();
				EasyMock.expectLastCall().andAnswer(() -> {
					e.writeMapStart();
					sizeWrittenSignal.countDown();
					try {
						eltAddedSignal.await();
					} catch (InterruptedException e) {
						// ignore
					}
					return null;
				}).andThrow(new IOException()).anyTimes();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		private void mockFlush() {
			try {
				this.MockedTestEncoder.flush();
				EasyMock.expectLastCall().andAnswer(() -> {
					e.flush();
					return null;
				}).andThrow(new IOException()).anyTimes();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		private void mockWriteNull() {
			try {
				this.MockedTestEncoder.writeNull();
				EasyMock.expectLastCall().andAnswer(() -> {
					e.writeNull();
					return null;
				}).andThrow(new IOException()).anyTimes();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		private void mockWriteBoolean() {
			try {
				this.MockedTestEncoder.writeBoolean(EasyMock.anyBoolean());
				EasyMock.expectLastCall().andAnswer(() -> {
					boolean b = EasyMock.getCurrentArgument(0);
					e.writeBoolean(b);
					return null;
				}).andThrow(new IOException()).anyTimes();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		private void mockWriteInt() {
			try {
				this.MockedTestEncoder.writeInt(EasyMock.anyInt());
				EasyMock.expectLastCall().andAnswer(() -> {
					int n = EasyMock.getCurrentArgument(0);
					e.writeInt(n);
					return null;
				}).andThrow(new IOException()).anyTimes();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		private void mockWriteLong() {
			try {
				this.MockedTestEncoder.writeLong(EasyMock.anyLong());
				EasyMock.expectLastCall().andAnswer(() -> {
					long n = EasyMock.getCurrentArgument(0);
					e.writeLong(n);
					return null;
				}).andThrow(new IOException()).anyTimes();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		private void mockWriteFloat() {
			try {
				this.MockedTestEncoder.writeFloat(EasyMock.anyFloat());
				EasyMock.expectLastCall().andAnswer(() -> {
					float n = EasyMock.getCurrentArgument(0);
					e.writeFloat(n);
					return null;
				}).andThrow(new IOException()).anyTimes();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		private void mockWriteDouble() {
			try {
				this.MockedTestEncoder.writeDouble(EasyMock.anyDouble());
				EasyMock.expectLastCall().andAnswer(() -> {
					double n = EasyMock.getCurrentArgument(0);
					e.writeDouble(n);
					return null;
				}).andThrow(new IOException()).anyTimes();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		private void mockWriteString() {
			try {
				this.MockedTestEncoder.writeString(EasyMock.anyObject(Utf8.class));
				EasyMock.expectLastCall().andAnswer(() -> {
					Utf8 utf8 = EasyMock.getCurrentArgument(0);
					e.writeString(utf8);
					return null;
				}).andThrow(new IOException()).anyTimes();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		private void mockWriteBytes1() {
			try {
				this.MockedTestEncoder.writeBytes(EasyMock.anyObject(ByteBuffer.class));
				EasyMock.expectLastCall().andAnswer(() -> {
					ByteBuffer bytes = EasyMock.getCurrentArgument(0);
					e.writeBytes(bytes);
					return null;
				}).andThrow(new IOException()).anyTimes();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		private void mockWriteBytes2() {
			try {
				this.MockedTestEncoder.writeBytes(EasyMock.anyObject(byte[].class), EasyMock.anyInt(), EasyMock.anyInt());
				EasyMock.expectLastCall().andAnswer(() -> {
					byte[] bytes = EasyMock.getCurrentArgument(0);
					int start = EasyMock.getCurrentArgument(1);
					int len = EasyMock.getCurrentArgument(2);
					e.writeBytes(bytes, start, len);
					return null;
				}).andThrow(new IOException()).anyTimes();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		private void mockWriteFixed() {
			try {
				this.MockedTestEncoder.writeFixed(EasyMock.anyObject(byte[].class), EasyMock.anyInt(), EasyMock.anyInt());
				EasyMock.expectLastCall().andAnswer(() -> {
					byte[] bytes = EasyMock.getCurrentArgument(0);
					int start = EasyMock.getCurrentArgument(1);
					int len = EasyMock.getCurrentArgument(2);
					e.writeBytes(bytes, start, len);
					return null;
				}).andThrow(new IOException()).anyTimes();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		private void mockWriteEnum() {
			try {
				this.MockedTestEncoder.writeEnum(EasyMock.anyInt());
				EasyMock.expectLastCall().andAnswer(() -> {
					int en = EasyMock.getCurrentArgument(0);
					e.writeEnum(en);
					return null;
				}).andThrow(new IOException()).anyTimes();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		private void mockSetItemCount() {
			try {
				this.MockedTestEncoder.setItemCount(EasyMock.anyLong());
				EasyMock.expectLastCall().andAnswer(() -> {
					long itemCount = EasyMock.getCurrentArgument(0);
					e.setItemCount(itemCount);
					return null;
				}).andThrow(new IOException()).anyTimes();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		private void mockStartItem() {
			try {
				this.MockedTestEncoder.startItem();
				EasyMock.expectLastCall().andAnswer(() -> {
					e.startItem();
					return null;
				}).andThrow(new IOException()).anyTimes();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		private void mockWriteArrayEnd() {
			try {
				this.MockedTestEncoder.writeArrayEnd();
				EasyMock.expectLastCall().andAnswer(() -> {
					e.writeArrayEnd();
					return null;
				}).andThrow(new IOException()).anyTimes();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		private void mockWriteMapEnd() {
			try {
				this.MockedTestEncoder.writeMapEnd();
				EasyMock.expectLastCall().andAnswer(() -> {
					e.writeMapEnd();
					return null;
				}).andThrow(new IOException()).anyTimes();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		private void mockWriteIndex() {
			try {
				this.MockedTestEncoder.writeIndex(EasyMock.anyInt());
				EasyMock.expectLastCall().andAnswer(() -> {
					int unionIndex = EasyMock.getCurrentArgument(0);
					e.writeIndex(unionIndex);
					return null;
				}).andThrow(new IOException()).anyTimes();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}

	@Test(expected = AvroTypeException.class)
	public void writeDoesNotAllowStringForGenericEnum() throws IOException {
		final String json = "{\"type\": \"record\", \"name\": \"recordWithEnum\"," + "\"fields\": [ " + "{\"name\": \"field\", \"type\": "
				+ "{\"type\": \"enum\", \"name\": \"enum\", \"symbols\": " + "[\"ONE\",\"TWO\",\"THREE\"] " + "}" + "}" + "]}";
		Schema schema = new Schema.Parser().parse(json);
		GenericRecord record = new GenericData.Record(schema);
		record.put("field", "ONE");

		ByteArrayOutputStream bao = new ByteArrayOutputStream();
		GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
		Encoder encoder = EncoderFactory.get().jsonEncoder(schema, bao);

		writer.write(record, encoder);
	}

	private enum AnEnum {
		ONE,
		TWO,
		THREE
	};

	@Test(expected = AvroTypeException.class)
	public void writeDoesNotAllowJavaEnumForGenericEnum() throws IOException {
		final String json = "{\"type\": \"record\", \"name\": \"recordWithEnum\"," + "\"fields\": [ " + "{\"name\": \"field\", \"type\": "
				+ "{\"type\": \"enum\", \"name\": \"enum\", \"symbols\": " + "[\"ONE\",\"TWO\",\"THREE\"] " + "}" + "}" + "]}";
		Schema schema = new Schema.Parser().parse(json);
		GenericRecord record = new GenericData.Record(schema);
		record.put("field", AnEnum.ONE);

		ByteArrayOutputStream bao = new ByteArrayOutputStream();
		GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
		Encoder encoder = EncoderFactory.get().jsonEncoder(schema, bao);

		writer.write(record, encoder);
	}

	@Test
	public void writeFieldWithDefaultWithExplicitNullDefaultInSchema() throws Exception {
		Schema schema = schemaWithExplicitNullDefault();
		GenericRecord record = createRecordWithDefaultField(schema);
		writeObject(schema, record);
	}

	@Test
	public void writeFieldWithDefaultWithoutExplicitNullDefaultInSchema() throws Exception {
		Schema schema = schemaWithoutExplicitNullDefault();
		GenericRecord record = createRecordWithDefaultField(schema);
		writeObject(schema, record);
	}

	private Schema schemaWithExplicitNullDefault() {
		String schema = "{\"type\":\"record\",\"name\":\"my_record\",\"namespace\":\"mytest.namespace\",\"doc\":\"doc\","
				+ "\"fields\":[{\"name\":\"f\",\"type\":[\"null\",\"string\"],\"doc\":\"field doc doc\", " + "\"default\":null}]}";
		return new Schema.Parser().parse(schema);
	}

	private Schema schemaWithoutExplicitNullDefault() {
		String schema = "{\"type\":\"record\",\"name\":\"my_record\",\"namespace\":\"mytest.namespace\",\"doc\":\"doc\","
				+ "\"fields\":[{\"name\":\"f\",\"type\":[\"null\",\"string\"],\"doc\":\"field doc doc\"}]}";
		return new Schema.Parser().parse(schema);
	}

	private void writeObject(Schema schema, GenericRecord datum) throws Exception {
		BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(new ByteArrayOutputStream(), null);
		GenericDatumWriter<GenericData.Record> writer = new GenericDatumWriter<>(schema);
		writer.write(schema, datum, encoder);
	}

	private GenericRecord createRecordWithDefaultField(Schema schema) {
		GenericRecord record = new GenericData.Record(schema);
		record.put("f", schema.getField("f").defaultVal());
		return record;
	}
}
