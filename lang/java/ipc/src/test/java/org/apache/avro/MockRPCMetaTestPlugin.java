package org.apache.avro;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.apache.avro.ipc.RPCContext;
import org.apache.avro.ipc.RPCPlugin;
import org.easymock.EasyMock;
import org.junit.Assert;

public class MockRPCMetaTestPlugin {
	public RPCPlugin MockedRPCMetaTestPlugin;

	protected final String key;

	public MockRPCMetaTestPlugin(String keyname) {
		key = keyname;
		this.MockedRPCMetaTestPlugin = EasyMock.partialMockBuilder(RPCPlugin.class).createMock();
		mockClientStartConnect();
		mockServerConnecting();
		mockClientFinishConnect();
		mockClientSendRequest();
		mockServerReceiveRequest();
		mockServerSendResponse();
		mockClientReceiveResponse();
		EasyMock.replay(this.MockedRPCMetaTestPlugin);
	}

	protected void checkRPCMetaMap(Map<String, ByteBuffer> rpcMeta) {
		Assert.assertNotNull(rpcMeta);
		Assert.assertTrue("key not present in map", rpcMeta.containsKey(key));

		ByteBuffer keybuf = rpcMeta.get(key);
		Assert.assertNotNull(keybuf);
		Assert.assertTrue("key BB had nothing remaining", keybuf.remaining() > 0);

		String str = new String(keybuf.array(), StandardCharsets.UTF_8);
		Assert.assertEquals("apache", str);
	}

	private void mockClientStartConnect() {
		this.MockedRPCMetaTestPlugin.clientStartConnect(EasyMock.anyObject(RPCContext.class));
		EasyMock.expectLastCall().andAnswer(() -> {
			RPCContext context = EasyMock.getCurrentArgument(0);
			ByteBuffer buf = ByteBuffer.wrap("ap".getBytes(StandardCharsets.UTF_8));
			context.requestHandshakeMeta().put(key, buf);
			return null;
		}).anyTimes();
	}

	private void mockServerConnecting() {
		this.MockedRPCMetaTestPlugin.serverConnecting(EasyMock.anyObject(RPCContext.class));
		EasyMock.expectLastCall().andAnswer(() -> {
			RPCContext context = EasyMock.getCurrentArgument(0);
			Assert.assertNotNull(context.requestHandshakeMeta());
			Assert.assertNotNull(context.responseHandshakeMeta());
			Assert.assertNull(context.getRequestPayload());
			Assert.assertNull(context.getResponsePayload());

			if (!context.requestHandshakeMeta().containsKey(key)) return null;

			ByteBuffer buf = context.requestHandshakeMeta().get(key);
			Assert.assertNotNull(buf);
			Assert.assertNotNull(buf.array());

			String partialstr = new String(buf.array(), StandardCharsets.UTF_8);
			Assert.assertNotNull(partialstr);
			Assert.assertEquals("partial string mismatch", "ap", partialstr);

			buf = ByteBuffer.wrap((partialstr + "ac").getBytes(StandardCharsets.UTF_8));
			Assert.assertTrue(buf.remaining() > 0);
			context.responseHandshakeMeta().put(key, buf);
			return null;
		}).anyTimes();
	}

	private void mockClientFinishConnect() {
		this.MockedRPCMetaTestPlugin.clientFinishConnect(EasyMock.anyObject(RPCContext.class));
		EasyMock.expectLastCall().andAnswer(() -> {
			RPCContext context = EasyMock.getCurrentArgument(0);
			Map<String, ByteBuffer> handshakeMeta = context.responseHandshakeMeta();

			Assert.assertNull(context.getRequestPayload());
			Assert.assertNull(context.getResponsePayload());
			Assert.assertNotNull(handshakeMeta);

			if (!handshakeMeta.containsKey(key)) return null;

			ByteBuffer buf = handshakeMeta.get(key);
			Assert.assertNotNull(buf);
			Assert.assertNotNull(buf.array());

			String partialstr = new String(buf.array(), StandardCharsets.UTF_8);
			Assert.assertNotNull(partialstr);
			Assert.assertEquals("partial string mismatch", "apac", partialstr);

			buf = ByteBuffer.wrap((partialstr + "he").getBytes(StandardCharsets.UTF_8));
			Assert.assertTrue(buf.remaining() > 0);
			handshakeMeta.put(key, buf);

			checkRPCMetaMap(handshakeMeta);
			return null;
		}).anyTimes();
	}

	private void mockClientSendRequest() {
		this.MockedRPCMetaTestPlugin.clientSendRequest(EasyMock.anyObject(RPCContext.class));
		EasyMock.expectLastCall().andAnswer(() -> {
			RPCContext context = EasyMock.getCurrentArgument(0);
			ByteBuffer buf = ByteBuffer.wrap("ap".getBytes(StandardCharsets.UTF_8));
			context.requestCallMeta().put(key, buf);
			Assert.assertNotNull(context.getMessage());
			Assert.assertNotNull(context.getRequestPayload());
			Assert.assertNull(context.getResponsePayload());
			return null;
		}).anyTimes();
	}

	private void mockServerReceiveRequest() {
		this.MockedRPCMetaTestPlugin.serverReceiveRequest(EasyMock.anyObject(RPCContext.class));
		EasyMock.expectLastCall().andAnswer(() -> {
			RPCContext context = EasyMock.getCurrentArgument(0);
			Map<String, ByteBuffer> meta = context.requestCallMeta();

			Assert.assertNotNull(meta);
			Assert.assertNotNull(context.getMessage());
			Assert.assertNull(context.getResponsePayload());

			if (!meta.containsKey(key)) return null;

			ByteBuffer buf = meta.get(key);
			Assert.assertNotNull(buf);
			Assert.assertNotNull(buf.array());

			String partialstr = new String(buf.array(), StandardCharsets.UTF_8);
			Assert.assertNotNull(partialstr);
			Assert.assertEquals("partial string mismatch", "ap", partialstr);

			buf = ByteBuffer.wrap((partialstr + "a").getBytes(StandardCharsets.UTF_8));
			Assert.assertTrue(buf.remaining() > 0);
			meta.put(key, buf);
			return null;
		}).anyTimes();
	}

	private void mockServerSendResponse() {
		this.MockedRPCMetaTestPlugin.serverSendResponse(EasyMock.anyObject(RPCContext.class));
		EasyMock.expectLastCall().andAnswer(() -> {
			RPCContext context = EasyMock.getCurrentArgument(0);
			Assert.assertNotNull(context.requestCallMeta());
			Assert.assertNotNull(context.responseCallMeta());

			Assert.assertNotNull(context.getResponsePayload());

			if (!context.requestCallMeta().containsKey(key)) return null;

			ByteBuffer buf = context.requestCallMeta().get(key);
			Assert.assertNotNull(buf);
			Assert.assertNotNull(buf.array());

			String partialstr = new String(buf.array(), StandardCharsets.UTF_8);
			Assert.assertNotNull(partialstr);
			Assert.assertEquals("partial string mismatch", "apa", partialstr);

			buf = ByteBuffer.wrap((partialstr + "c").getBytes(StandardCharsets.UTF_8));
			Assert.assertTrue(buf.remaining() > 0);
			context.responseCallMeta().put(key, buf);
			return null;
		}).anyTimes();
	}

	private void mockClientReceiveResponse() {
		this.MockedRPCMetaTestPlugin.clientReceiveResponse(EasyMock.anyObject(RPCContext.class));
		EasyMock.expectLastCall().andAnswer(() -> {
			RPCContext context = EasyMock.getCurrentArgument(0);
			Assert.assertNotNull(context.responseCallMeta());
			Assert.assertNotNull(context.getRequestPayload());

			if (!context.responseCallMeta().containsKey(key)) return null;

			ByteBuffer buf = context.responseCallMeta().get(key);
			Assert.assertNotNull(buf);
			Assert.assertNotNull(buf.array());

			String partialstr = new String(buf.array(), StandardCharsets.UTF_8);
			Assert.assertNotNull(partialstr);
			Assert.assertEquals("partial string mismatch", "apac", partialstr);

			buf = ByteBuffer.wrap((partialstr + "he").getBytes(StandardCharsets.UTF_8));
			Assert.assertTrue(buf.remaining() > 0);
			context.responseCallMeta().put(key, buf);

			checkRPCMetaMap(context.responseCallMeta());
			return null;
		}).anyTimes();
	}

}
