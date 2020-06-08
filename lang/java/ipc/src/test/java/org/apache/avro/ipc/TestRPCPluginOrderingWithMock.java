package org.apache.avro.ipc;

import static org.junit.Assert.assertEquals;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.avro.ipc.specific.SpecificRequestor;
import org.apache.avro.ipc.specific.SpecificResponder;
import org.apache.avro.test.Mail;
import org.apache.avro.test.Message;
import org.easymock.EasyMock;
import org.junit.Test;

public class TestRPCPluginOrderingWithMock {
	private static AtomicInteger orderCounter = new AtomicInteger();

	public class MockOrderPlugin {

		public RPCPlugin MockedOrderPlugin;

		public MockOrderPlugin() {
			this.MockedOrderPlugin = EasyMock.partialMockBuilder(RPCPlugin.class).addMockedMethod("clientStartConnect", RPCContext.class)
					.addMockedMethod("clientSendRequest", RPCContext.class).addMockedMethod("clientReceiveResponse", RPCContext.class)
					.addMockedMethod("serverConnecting", RPCContext.class).addMockedMethod("clientFinishConnect", RPCContext.class)
					.addMockedMethod("serverReceiveRequest", RPCContext.class).addMockedMethod("serverSendResponse", RPCContext.class).createMock();
			mockClientStartConnect();
			mockClientSendRequest();
			mockClientReceiveResponse();
			mockClientFinishConnect();
			mockServerConnecting();
			mockServerReceiveRequest();
			mockServerSendResponse();
			EasyMock.replay(this.MockedOrderPlugin);
		}

		private void mockClientStartConnect() {
			this.MockedOrderPlugin.clientStartConnect(EasyMock.anyObject(RPCContext.class));
			EasyMock.expectLastCall().andAnswer(() -> {
				System.out.println("Client Start Connect: " + orderCounter.get());
				assertEquals(0, orderCounter.getAndIncrement());
				return null;
			}).anyTimes();
		}

		private void mockClientSendRequest() {
			this.MockedOrderPlugin.clientSendRequest(EasyMock.anyObject(RPCContext.class));
			EasyMock.expectLastCall().andAnswer(() -> {
				System.out.println("Client Send Request: " + orderCounter.get());
				assertEquals(1, orderCounter.getAndIncrement());
				return null;
			}).anyTimes();
		}

		private void mockClientReceiveResponse() {
			this.MockedOrderPlugin.clientReceiveResponse(EasyMock.anyObject(RPCContext.class));
			EasyMock.expectLastCall().andAnswer(() -> {
				System.out.println("Client Receive Response: " + orderCounter.get());
				assertEquals(6, orderCounter.getAndIncrement());
				return null;
			}).anyTimes();
		}

		private void mockClientFinishConnect() {
			this.MockedOrderPlugin.clientFinishConnect(EasyMock.anyObject(RPCContext.class));
			EasyMock.expectLastCall().andAnswer(() -> {
				System.out.println("Client Finish Connect: " + orderCounter.get());
				assertEquals(5, orderCounter.getAndIncrement());
				return null;
			}).anyTimes();
		}

		private void mockServerConnecting() {
			this.MockedOrderPlugin.serverConnecting(EasyMock.anyObject(RPCContext.class));
			EasyMock.expectLastCall().andAnswer(() -> {
				System.out.println("Server Start Connect: " + orderCounter.get());
				assertEquals(2, orderCounter.getAndIncrement());
				return null;
			}).anyTimes();
		}

		private void mockServerReceiveRequest() {
			this.MockedOrderPlugin.serverReceiveRequest(EasyMock.anyObject(RPCContext.class));
			EasyMock.expectLastCall().andAnswer(() -> {
				System.out.println("Server Receive Request: " + orderCounter.get());
				assertEquals(3, orderCounter.getAndIncrement());
				return null;
			}).anyTimes();
		}

		private void mockServerSendResponse() {
			this.MockedOrderPlugin.serverSendResponse(EasyMock.anyObject(RPCContext.class));
			EasyMock.expectLastCall().andAnswer(() -> {
				System.out.println("Server Send Response: " + orderCounter.get());
				assertEquals(4, orderCounter.getAndIncrement());
				return null;
			}).anyTimes();
		}

	}

	@Test
	public void testRpcPluginOrdering() throws Exception {
		RPCPlugin plugin = new MockOrderPlugin().MockedOrderPlugin;

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
