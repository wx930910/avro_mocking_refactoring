package org.apache.avro.file;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.easymock.Capture;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.junit.Test;

public class TestCustomCodecWithMock {

	/**
	 * {@link MockCustomCodecUtility} define all new methods defined in
	 * {@link org.apache.avro.file.codec.CustomCodec}
	 * 
	 * @author Twilight
	 *
	 */
	private static class MockCustomCodecUtility {

		public static boolean compareDecompress(Codec MockCustomCodec, Codec other, ByteBuffer original) throws IOException {
			ByteBuffer compressedA = MockCustomCodec.compress(original);
			original.rewind();
			ByteBuffer compressedB = other.compress(original);

			return MockCustomCodec.decompress(compressedA).equals(other.decompress((ByteBuffer) compressedA.rewind()))
					&& MockCustomCodec.decompress(compressedB).equals(other.decompress((ByteBuffer) compressedB.rewind()));
		}

	}

	private Codec setupMockCustomCodec() throws IOException {
		String CODECNAME = "CUSTOMCODEC";
		Codec MockCustomCodec = EasyMock.partialMockBuilder(Codec.class).addMockedMethod("hashCode").createMock();
		/**
		 * Mock @see org.apache.avro.file.codec.CustomCodec#getName()
		 */
		EasyMock.expect(MockCustomCodec.getName()).andReturn(CODECNAME);
		/**
		 * Mock @see org.apache.avro.file.CustomCodec#compress(ByteBuffer)
		 */
		Capture<ByteBuffer> captureMethodCompressParameterByteBuffer = EasyMock.newCapture();
		EasyMock.expect(MockCustomCodec.compress(EasyMock.capture(captureMethodCompressParameterByteBuffer))).andAnswer(new IAnswer<ByteBuffer>() {
			@Override
			public ByteBuffer answer() throws Throwable, IOException {
				ByteBuffer in = captureMethodCompressParameterByteBuffer.getValue();
				ByteBuffer out = ByteBuffer.allocate(in.remaining());
				while (in.position() < in.capacity())
					out.put((byte) ~in.get());
				return out;
			}
		});
		/**
		 * Mock @see org.apache.avro.file.CustomCodec#decompress(ByteBuffer)
		 */
		Capture<ByteBuffer> captureMethodDecompressParameterByteBuffer = EasyMock.newCapture();
		EasyMock.expect(MockCustomCodec.decompress(EasyMock.capture(captureMethodDecompressParameterByteBuffer)))
				.andAnswer(new IAnswer<ByteBuffer>() {
					@Override
					public ByteBuffer answer() throws Throwable, IOException {
						ByteBuffer in = captureMethodDecompressParameterByteBuffer.getValue();
						ByteBuffer out = ByteBuffer.allocate(in.remaining());
						while (in.position() < in.capacity())
							out.put((byte) ~in.get());
						return out;
					}
				});

		// /**
		// * Mock @see org.apache.avro.file.CustomCodec#equals(Object)
		// */
		// Capture<Object> captureMethodEqualsParameterObject =
		// EasyMock.newCapture();
		// EasyMock.expect(MockCustomCodec.equals(EasyMock.capture(captureMethodEqualsParameterObject))).andAnswer(new
		// IAnswer<Boolean>() {
		// @Override
		// public Boolean answer() throws Throwable {
		// Object other = captureMethodEqualsParameterObject.getValue();
		// if (this == other) return true;
		// if (other instanceof Codec) {
		// ByteBuffer original =
		// ByteBuffer.allocate(MockCustomCodec.getName().getBytes(UTF_8).length);
		// original.put(MockCustomCodec.getName().getBytes(UTF_8));
		// original.rewind();
		// try {
		// return MockCustomCodecUtility.compareDecompress(MockCustomCodec,
		// (Codec) other, original);
		// } catch (IOException e) {
		// return false;
		// }
		// } else return false;
		// }
		// });

		/**
		 * Mock @see org.apache.avro.file.CustomCodec#hashCode()
		 */

		EasyMock.expect(MockCustomCodec.hashCode()).andStubReturn(10);

		EasyMock.replay(MockCustomCodec);

		return MockCustomCodec;
	}

	@Test
	public void testCustomCodec() throws IOException {
		Codec customCodec = setupMockCustomCodec();
		Codec snappyCodec = new SnappyCodec.Option().createInstance();
		assertTrue(customCodec.equals(setupMockCustomCodec()));
		assertFalse(customCodec.equals(snappyCodec));

		String testString = "Testing 123";
		ByteBuffer original = ByteBuffer.allocate(testString.getBytes(UTF_8).length);
		original.put(testString.getBytes(UTF_8));
		original.rewind();
		ByteBuffer decompressed = null;
		try {
			ByteBuffer compressed = customCodec.compress(original);
			compressed.rewind();
			decompressed = customCodec.decompress(compressed);
		} catch (IOException e) {
			e.printStackTrace();
		}

		assertEquals(testString, new String(decompressed.array(), UTF_8));

	}
}
