package org.apache.avro.codegentest;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.avro.Conversion;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.easymock.IAnswer;

public class MockCustomDecimalConversion {

	public Conversion<CustomDecimal> MockedCustomDecimalConversion;

	public MockCustomDecimalConversion() {
		this.MockedCustomDecimalConversion = EasyMock.partialMockBuilder(Conversion.class).addMockedMethod("getConvertedType")
				.addMockedMethod("fromBytes", ByteBuffer.class, Schema.class, LogicalType.class)
				.addMockedMethod("toBytes", Object.class, Schema.class, LogicalType.class)
				.addMockedMethod("fromFixed", GenericFixed.class, Schema.class, LogicalType.class)
				.addMockedMethod("toFixed", Object.class, Schema.class, LogicalType.class).createMock();
		mockGetLogicalTypeName();
		mockGetConvertedType();

		mockFromBytes();
		mockToBytes();
		mockFromFixed();
		mockToFixed();
		EasyMock.replay(this.MockedCustomDecimalConversion);
	}

	private void mockGetConvertedType() {
		EasyMock.expect(this.MockedCustomDecimalConversion.getConvertedType()).andReturn(CustomDecimal.class).anyTimes();
	}

	private void mockGetLogicalTypeName() {
		EasyMock.expect(this.MockedCustomDecimalConversion.getLogicalTypeName()).andReturn("decimal").anyTimes();
	}

	private void mockFromBytes() {
		Capture<ByteBuffer> captureValue = EasyMock.newCapture();
		Capture<LogicalType> captureType = EasyMock.newCapture();
		EasyMock.expect(this.MockedCustomDecimalConversion.fromBytes(EasyMock.capture(captureValue), EasyMock.anyObject(Schema.class),
				EasyMock.capture(captureType))).andAnswer(new IAnswer<CustomDecimal>() {
					@Override
					public CustomDecimal answer() {
						ByteBuffer value = captureValue.getValue();
						LogicalType type = captureType.getValue();
						int scale = ((LogicalTypes.Decimal) type).getScale();
						byte[] bytes = value.get(new byte[value.remaining()]).array();
						return new CustomDecimal(new BigInteger(bytes), scale);
					}
				}).anyTimes();
	}

	private void mockToBytes() {
		Capture<CustomDecimal> captureValue = EasyMock.newCapture();
		Capture<LogicalType> captureType = EasyMock.newCapture();
		EasyMock.expect(this.MockedCustomDecimalConversion.toBytes(EasyMock.capture(captureValue), EasyMock.anyObject(Schema.class),
				EasyMock.capture(captureType))).andAnswer(new IAnswer<ByteBuffer>() {
					@Override
					public ByteBuffer answer() {
						CustomDecimal value = captureValue.getValue();
						LogicalType type = captureType.getValue();
						int scale = ((LogicalTypes.Decimal) type).getScale();
						return ByteBuffer.wrap(value.toByteArray(scale));
					}
				}).anyTimes();
	}

	private void mockFromFixed() {
		Capture<GenericFixed> captureValue = EasyMock.newCapture();
		Capture<LogicalType> captureType = EasyMock.newCapture();
		EasyMock.expect(this.MockedCustomDecimalConversion.fromFixed(EasyMock.capture(captureValue), EasyMock.anyObject(Schema.class),
				EasyMock.capture(captureType))).andAnswer(new IAnswer<CustomDecimal>() {
					@Override
					public CustomDecimal answer() {
						GenericFixed value = captureValue.getValue();
						LogicalType type = captureType.getValue();
						int scale = ((LogicalTypes.Decimal) type).getScale();
						return new CustomDecimal(new BigInteger(value.bytes()), scale);
					}
				}).anyTimes();
	}

	private void mockToFixed() {
		Capture<CustomDecimal> captureValue = EasyMock.newCapture();
		Capture<Schema> captureSchema = EasyMock.newCapture();
		Capture<LogicalType> captureType = EasyMock.newCapture();
		EasyMock.expect(this.MockedCustomDecimalConversion.toFixed(EasyMock.capture(captureValue), EasyMock.capture(captureSchema),
				EasyMock.capture(captureType))).andAnswer(new IAnswer<GenericFixed>() {
					@Override
					public GenericFixed answer() {
						CustomDecimal value = captureValue.getValue();
						Schema schema = captureSchema.getValue();
						LogicalType type = captureType.getValue();
						int scale = ((LogicalTypes.Decimal) type).getScale();
						byte fillByte = (byte) (value.signum() < 0 ? 0xFF : 0x00);
						byte[] unscaled = value.toByteArray(scale);
						byte[] bytes = new byte[schema.getFixedSize()];
						int offset = bytes.length - unscaled.length;

						// Fill the front of the array and copy remaining with
						// unscaled values
						Arrays.fill(bytes, 0, offset, fillByte);
						System.arraycopy(unscaled, 0, bytes, offset, bytes.length - offset);

						return new GenericData.Fixed(schema, bytes);
					}
				}).anyTimes();
	}

}
