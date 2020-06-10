package org.apache.avro.io;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.apache.avro.util.Utf8;
import org.easymock.EasyMock;

public class MockLegacyBinaryEncoder {
	public Encoder MockedLegacyBinaryEncoder;
	protected OutputStream out;

	private interface ByteWriter {
		void write(ByteBuffer bytes) throws IOException;
	}

	private static final class SimpleByteWriter implements ByteWriter {
		private final OutputStream out;

		public SimpleByteWriter(OutputStream out) {
			this.out = out;
		}

		@Override
		public void write(ByteBuffer bytes) throws IOException {
			encodeLong(bytes.remaining(), out);
			out.write(bytes.array(), bytes.position(), bytes.remaining());
		}
	}

	protected static void encodeLong(long n, OutputStream o) throws IOException {
		n = (n << 1) ^ (n >> 63); // move sign to low-order bit
		while ((n & ~0x7F) != 0) {
			o.write((byte) ((n & 0x7f) | 0x80));
			n >>>= 7;
		}
		o.write((byte) n);
	}

	protected static void encodeFloat(float f, OutputStream o) throws IOException {
		long bits = Float.floatToRawIntBits(f);
		o.write((int) (bits) & 0xFF);
		o.write((int) (bits >> 8) & 0xFF);
		o.write((int) (bits >> 16) & 0xFF);
		o.write((int) (bits >> 24) & 0xFF);
	}

	protected static void encodeDouble(double d, OutputStream o) throws IOException {
		long bits = Double.doubleToRawLongBits(d);
		o.write((int) (bits) & 0xFF);
		o.write((int) (bits >> 8) & 0xFF);
		o.write((int) (bits >> 16) & 0xFF);
		o.write((int) (bits >> 24) & 0xFF);
		o.write((int) (bits >> 32) & 0xFF);
		o.write((int) (bits >> 40) & 0xFF);
		o.write((int) (bits >> 48) & 0xFF);
		o.write((int) (bits >> 56) & 0xFF);
	}

	private final ByteWriter byteWriter;

	public MockLegacyBinaryEncoder(OutputStream out) {
		this.out = out;
		this.byteWriter = new SimpleByteWriter(out);
		this.MockedLegacyBinaryEncoder = EasyMock.partialMockBuilder(Encoder.class).addMockedMethod("writeString", String.class)
				.addMockedMethod("writeString", Utf8.class).createMock();
		mockFlush();
		mockWriteNull();
		mockWriteBoolean();
		mockWriteInt();
		mockWriteLong();
		mockWriteFloat();
		mockWriteDouble();
		mockWriteString1();
		mockWriteString2();
		mockWriteBytes1();
		mockWriteBytes2();
		mockWriteFixed();
		mockWriteEnum();
		mockWriteArrayStart();
		mockSetItemCount();
		mockStartItem();
		mockWriteArrayEnd();
		mockWriteMapStart();
		mockWriteMapEnd();
		mockWriteIndex();
		EasyMock.replay(this.MockedLegacyBinaryEncoder);
	}

	private void mockFlush() {
		try {
			this.MockedLegacyBinaryEncoder.flush();
			EasyMock.expectLastCall().andAnswer(() -> {
				if (out != null) {
					out.flush();
				}
				return null;
			}).anyTimes();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void mockWriteNull() {
		try {
			this.MockedLegacyBinaryEncoder.writeNull();
			EasyMock.expectLastCall().andAnswer(() -> {
				return null;
			}).anyTimes();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void mockWriteBoolean() {
		try {
			this.MockedLegacyBinaryEncoder.writeBoolean(EasyMock.anyBoolean());
			EasyMock.expectLastCall().andAnswer(() -> {
				boolean b = EasyMock.getCurrentArgument(0);
				out.write(b ? 1 : 0);
				return null;
			}).anyTimes();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void mockWriteInt() {
		try {
			this.MockedLegacyBinaryEncoder.writeInt(EasyMock.anyInt());
			EasyMock.expectLastCall().andAnswer(() -> {
				int n = EasyMock.getCurrentArgument(0);
				encodeLong(n, out);
				return null;
			}).anyTimes();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void mockWriteLong() {
		try {
			this.MockedLegacyBinaryEncoder.writeLong(EasyMock.anyLong());
			EasyMock.expectLastCall().andAnswer(() -> {
				long n = EasyMock.getCurrentArgument(0);
				encodeLong(n, out);
				return null;
			}).anyTimes();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void mockWriteFloat() {
		try {
			this.MockedLegacyBinaryEncoder.writeFloat(EasyMock.anyFloat());
			EasyMock.expectLastCall().andAnswer(() -> {
				float f = EasyMock.getCurrentArgument(0);
				encodeFloat(f, out);
				return null;
			}).anyTimes();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void mockWriteDouble() {
		try {
			this.MockedLegacyBinaryEncoder.writeDouble(EasyMock.anyDouble());
			EasyMock.expectLastCall().andAnswer(() -> {
				double d = EasyMock.getCurrentArgument(0);
				encodeDouble(d, out);
				return null;
			}).anyTimes();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void mockWriteString1() {
		try {
			this.MockedLegacyBinaryEncoder.writeString(EasyMock.anyObject(Utf8.class));
			EasyMock.expectLastCall().andAnswer(() -> {
				Utf8 utf8 = EasyMock.getCurrentArgument(0);
				encodeString(utf8.getBytes(), 0, utf8.getByteLength());
				return null;
			}).anyTimes();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void mockWriteString2() {
		try {
			this.MockedLegacyBinaryEncoder.writeString(EasyMock.anyString());
			EasyMock.expectLastCall().andAnswer(() -> {
				String string = EasyMock.getCurrentArgument(0);
				byte[] bytes = Utf8.getBytesFor(string);
				encodeString(bytes, 0, bytes.length);
				return null;
			}).anyTimes();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void encodeString(byte[] bytes, int offset, int length) throws IOException {
		encodeLong(length, out);
		out.write(bytes, offset, length);
	}

	private void mockWriteBytes1() {
		try {
			this.MockedLegacyBinaryEncoder.writeBytes(EasyMock.anyObject(ByteBuffer.class));
			EasyMock.expectLastCall().andAnswer(() -> {
				ByteBuffer bytes = EasyMock.getCurrentArgument(0);
				byteWriter.write(bytes);
				return null;
			}).anyTimes();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void mockWriteBytes2() {
		try {
			this.MockedLegacyBinaryEncoder.writeBytes(EasyMock.anyObject(byte[].class), EasyMock.anyInt(), EasyMock.anyInt());
			EasyMock.expectLastCall().andAnswer(() -> {
				byte[] bytes = EasyMock.getCurrentArgument(0);
				int start = EasyMock.getCurrentArgument(1);
				int len = EasyMock.getCurrentArgument(2);
				encodeLong(len, out);
				out.write(bytes, start, len);
				return null;
			}).anyTimes();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void mockWriteFixed() {
		try {
			this.MockedLegacyBinaryEncoder.writeFixed(EasyMock.anyObject(byte[].class), EasyMock.anyInt(), EasyMock.anyInt());
			EasyMock.expectLastCall().andAnswer(() -> {
				byte[] bytes = EasyMock.getCurrentArgument(0);
				int start = EasyMock.getCurrentArgument(1);
				int len = EasyMock.getCurrentArgument(2);
				out.write(bytes, start, len);
				return null;
			}).anyTimes();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void mockWriteEnum() {
		try {
			this.MockedLegacyBinaryEncoder.writeEnum(EasyMock.anyInt());
			EasyMock.expectLastCall().andAnswer(() -> {
				int e = EasyMock.getCurrentArgument(0);
				encodeLong(e, out);
				return null;
			}).anyTimes();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void mockWriteArrayStart() {
		try {
			this.MockedLegacyBinaryEncoder.writeArrayStart();
			EasyMock.expectLastCall().andAnswer(() -> {
				return null;
			}).anyTimes();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void mockSetItemCount() {
		try {
			this.MockedLegacyBinaryEncoder.setItemCount(EasyMock.anyLong());
			EasyMock.expectLastCall().andAnswer(() -> {
				long itemCount = EasyMock.getCurrentArgument(0);
				if (itemCount > 0) {
					this.MockedLegacyBinaryEncoder.writeLong(itemCount);
				}
				return null;
			}).anyTimes();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void mockStartItem() {
		try {
			this.MockedLegacyBinaryEncoder.startItem();
			EasyMock.expectLastCall().andAnswer(() -> {
				return null;
			}).anyTimes();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void mockWriteArrayEnd() {
		try {
			this.MockedLegacyBinaryEncoder.writeArrayEnd();
			EasyMock.expectLastCall().andAnswer(() -> {
				encodeLong(0, out);
				return null;
			}).anyTimes();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void mockWriteMapStart() {
		try {
			this.MockedLegacyBinaryEncoder.writeMapStart();
			EasyMock.expectLastCall().andAnswer(() -> {
				return null;
			}).anyTimes();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void mockWriteMapEnd() {
		try {
			this.MockedLegacyBinaryEncoder.writeMapEnd();
			EasyMock.expectLastCall().andAnswer(() -> {
				encodeLong(0, out);
				return null;
			}).anyTimes();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void mockWriteIndex() {
		try {
			this.MockedLegacyBinaryEncoder.writeIndex(EasyMock.anyInt());
			EasyMock.expectLastCall().andAnswer(() -> {
				int unionIndex = EasyMock.getCurrentArgument(0);
				encodeLong(unionIndex, out);
				return null;
			}).anyTimes();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
