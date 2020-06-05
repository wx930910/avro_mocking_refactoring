package org.apache.avro.ipc;

import static org.junit.Assert.assertEquals;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.avro.ipc.specific.SpecificRequestor;
import org.apache.avro.ipc.specific.SpecificResponder;
import org.apache.avro.test.Mail;
import org.apache.avro.test.Message;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

public class TestRPCPluginOrderingWithMock {
	private static AtomicInteger orderCounter = new AtomicInteger();
	private RPCPlugin mockRPCPlugin;

	@Before
	public void setup() {
		this.mockRPCPlugin = EasyMock.createNiceMock(RPCPlugin.class);
		this.mockRPCPlugin.clientStartConnect(EasyMock.anyObject(RPCContext.class));
		EasyMock.expectLastCall().andAnswer(() -> {
			return orderCounter.getAndIncrement();
		});
		// EasyMock.expect(this.mockRPCPlugin.clientStartConnect(EasyMock.anyObject(RPCContext.class)));
		this.mockRPCPlugin.clientStartConnect(EasyMock.anyObject(RPCContext.class));
		// EasyMock.expectLastCall().andAnswer(answer);
	}

	public class OrderPlugin extends RPCPlugin {

		public void clientStartConnect(RPCContext context) {
			assertEquals(0, orderCounter.getAndIncrement());
		}

		public void clientSendRequest(RPCContext context) {
			assertEquals(1, orderCounter.getAndIncrement());
		}

		public void clientReceiveResponse(RPCContext context) {
			assertEquals(6, orderCounter.getAndIncrement());
		}

		public void clientFinishConnect(RPCContext context) {
			assertEquals(5, orderCounter.getAndIncrement());
		}

		public void serverConnecting(RPCContext context) {
			assertEquals(2, orderCounter.getAndIncrement());
		}

		public void serverReceiveRequest(RPCContext context) {
			assertEquals(3, orderCounter.getAndIncrement());
		}

		public void serverSendResponse(RPCContext context) {
			assertEquals(4, orderCounter.getAndIncrement());
		}
	}

	@Test
	public void testRpcPluginOrdering() throws Exception {
		OrderPlugin plugin = new OrderPlugin();

		SpecificResponder responder = new SpecificResponder(Mail.class, new TestMailImpl());
		SpecificRequestor requestor = new SpecificRequestor(Mail.class, new LocalTransceiver(responder));
		responder.addRPCPlugin(plugin);
		requestor.addRPCPlugin(plugin);

		Mail client = SpecificRequestor.getClient(Mail.class, requestor);
		Message message = createTestMessage();
		client.send(message);
	}

	private Message createTestMessage() {
		Message message = Message.newBuilder().setTo("me@test.com").setFrom("you@test.com").setBody("plugin testing").build();
		return message;
	}

	private static class TestMailImpl implements Mail {
		public String send(Message message) {
			return "Received";
		}

		public void fireandforget(Message message) {
		}
	}
}
