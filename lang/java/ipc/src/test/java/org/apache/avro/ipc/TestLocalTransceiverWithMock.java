package org.apache.avro.ipc;

import static org.junit.Assert.assertEquals;

import org.apache.avro.AvroRemoteException;
import org.apache.avro.Protocol;
import org.apache.avro.Protocol.Message;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.ipc.generic.GenericRequestor;
import org.apache.avro.ipc.generic.GenericResponder;
import org.apache.avro.util.Utf8;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.junit.Test;

public class TestLocalTransceiverWithMock {
	Protocol protocol = Protocol.parse("" + "{\"protocol\": \"Minimal\", " + "\"messages\": { \"m\": {"
			+ "   \"request\": [{\"name\": \"x\", \"type\": \"string\"}], " + "   \"response\": \"string\"} } }");

	static GenericResponder MockGenericResponder(Protocol local) throws Exception {
		GenericResponder MockResponder = EasyMock.partialMockBuilder(GenericResponder.class).withConstructor(Protocol.class).withArgs(local)
				.createMock();

		Capture<Object> respondMethodRequestCapture = EasyMock.newCapture();
		EasyMock.expect(MockResponder.respond(EasyMock.anyObject(Message.class), EasyMock.capture(respondMethodRequestCapture)))
				.andAnswer(new IAnswer<Object>() {
					@Override
					public Object answer() {
						assertEquals(new Utf8("hello"), ((GenericRecord) respondMethodRequestCapture.getValue()).get("x"));
						return new Utf8("there");
					}
				}).anyTimes();
		EasyMock.replay(MockResponder);
		return MockResponder;
	}

	static class TestResponder extends GenericResponder {
		public TestResponder(Protocol local) {
			super(local);
		}

		@Override
		public Object respond(Message message, Object request) throws AvroRemoteException {
			assertEquals(new Utf8("hello"), ((GenericRecord) request).get("x"));
			return new Utf8("there");
		}

	}

	@Test
	public void testSingleRpc() throws Exception {
		Transceiver t = new LocalTransceiver(MockGenericResponder(protocol));
		GenericRecord params = new GenericData.Record(protocol.getMessages().get("m").getRequest());
		params.put("x", new Utf8("hello"));
		GenericRequestor r = new GenericRequestor(protocol, t);

		for (int x = 0; x < 5; x++)
			assertEquals(new Utf8("there"), r.request("m", params));
	}
}
