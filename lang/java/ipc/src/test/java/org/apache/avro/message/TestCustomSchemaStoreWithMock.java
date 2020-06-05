package org.apache.avro.message;

import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;

import org.apache.avro.Schema;
import org.apache.avro.compiler.schema.evolve.NestedEvolve1;
import org.apache.avro.compiler.schema.evolve.NestedEvolve2;
import org.apache.avro.compiler.schema.evolve.NestedEvolve3;
import org.apache.avro.compiler.schema.evolve.TestRecord2;
import org.apache.avro.compiler.schema.evolve.TestRecord3;
import org.apache.avro.message.SchemaStore.Cache;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestCustomSchemaStoreWithMock {

	private static Cache cache = new Cache();
	static {
		cache.addSchema(NestedEvolve1.getClassSchema());
		cache.addSchema(NestedEvolve2.getClassSchema());
	}
	private static SchemaStore mockSchemaStore;

	@BeforeClass
	public static void setup() {
		mockSchemaStore = EasyMock.createNiceMock(SchemaStore.class);
		Capture<Long> fingerprint = EasyMock.newCapture();
		EasyMock.expect(mockSchemaStore.findByFingerprint(EasyMock.captureLong(fingerprint))).andAnswer(new IAnswer<Schema>() {
			@Override
			public Schema answer() throws Throwable {
				return cache.findByFingerprint(fingerprint.getValue());
			}
		}).times(1);
		EasyMock.replay(mockSchemaStore);
		decoder = NestedEvolve1.createDecoder(mockSchemaStore);
	}

	private static BinaryMessageDecoder<NestedEvolve1> decoder;

	@Test
	public void testCompatibleReadWithSchemaFromSchemaStoreWithMockingFrameWork() throws Exception {
		// Create and encode a NestedEvolve2 record.
		NestedEvolve2.Builder rootBuilder = NestedEvolve2.newBuilder().setRootName("RootName");
		rootBuilder.setNested(TestRecord2.newBuilder().setName("Name").setValue(1).setData("Data").build());
		ByteBuffer nestedEvolve2Buffer = rootBuilder.build().toByteBuffer();

		// Decode it
		NestedEvolve1 nestedEvolve1 = decoder.decode(nestedEvolve2Buffer);

		// Should work
		assertEquals(nestedEvolve1.getRootName(), "RootName");
		assertEquals(nestedEvolve1.getNested().getName(), "Name");
		assertEquals(nestedEvolve1.getNested().getValue(), 1);
		EasyMock.verify(mockSchemaStore);
	}

	@Test(expected = MissingSchemaException.class)
	public void testIncompatibleReadWithSchemaFromSchemaStoreWithMockingFrameWork() throws Exception {
		// Create and encode a NestedEvolve3 record.
		NestedEvolve3.Builder rootBuilder = NestedEvolve3.newBuilder().setRootName("RootName");
		rootBuilder.setNested(TestRecord3.newBuilder().setName("Name").setData("Data").build());
		ByteBuffer nestedEvolve3Buffer = rootBuilder.build().toByteBuffer();

		// Decode it ... should fail because schema for 'NestedEvolve3' is not
		// available
		// in the SchemaStore
		decoder.decode(nestedEvolve3Buffer);
		EasyMock.verify(mockSchemaStore);
	}

}
