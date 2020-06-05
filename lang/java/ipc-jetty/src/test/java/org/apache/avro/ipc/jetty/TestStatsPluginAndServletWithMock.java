package org.apache.avro.ipc.jetty;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.StringWriter;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.Random;

import javax.servlet.UnavailableException;

import org.apache.avro.AvroRemoteException;
import org.apache.avro.Protocol;
import org.apache.avro.Protocol.Message;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.ipc.HttpTransceiver;
import org.apache.avro.ipc.LocalTransceiver;
import org.apache.avro.ipc.RPCContext;
import org.apache.avro.ipc.Responder;
import org.apache.avro.ipc.Transceiver;
import org.apache.avro.ipc.generic.GenericRequestor;
import org.apache.avro.ipc.generic.GenericResponder;
import org.apache.avro.ipc.stats.StatsPlugin;
import org.apache.avro.ipc.stats.StatsServlet;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.junit.Before;
import org.junit.Test;

public class TestStatsPluginAndServletWithMock {
	Protocol protocol = Protocol.parse("" + "{\"protocol\": \"Minimal\", " + "\"messages\": { \"m\": {"
			+ "   \"request\": [{\"name\": \"x\", \"type\": \"int\"}], " + "   \"response\": \"int\"} } }");
	Message message = protocol.getMessages().get("m");

	private static final long MS = 1000 * 1000L;

	/**
	 * Mock TestResponder
	 */
	private GenericResponder MockTestResponder;

	@Before
	public void setupTestResponder() throws Exception {
		MockTestResponder = EasyMock.partialMockBuilder(GenericResponder.class).withConstructor(Protocol.class).withArgs(this.protocol).createMock();

		Capture<Object> requestCapture = EasyMock.newCapture();
		EasyMock.expect(this.MockTestResponder.respond(EasyMock.anyObject(Message.class), EasyMock.capture(requestCapture)))
				.andAnswer(new IAnswer<Object>() {
					@Override
					public Object answer() throws Throwable {
						assertEquals(0, ((GenericRecord) requestCapture.getValue()).get("x"));
						return 1;
					}
				}).anyTimes();
		EasyMock.replay(this.MockTestResponder);
	}

	/**
	 * Mock SleepyResponder
	 */
	/**
	 * Sleeps as requested.
	 * 
	 * @throws Exception
	 */

	public static GenericResponder setupSleepyResponder(Protocol protocol) throws Exception {
		GenericResponder MockSleepyResponder = EasyMock.partialMockBuilder(GenericResponder.class).withConstructor(Protocol.class).withArgs(protocol)
				.createMock();
		Capture<Object> requestCapture = EasyMock.newCapture();
		EasyMock.expect(MockSleepyResponder.respond(EasyMock.anyObject(Message.class), EasyMock.capture(requestCapture)))
				.andAnswer(new IAnswer<Object>() {
					@Override
					public Object answer() throws Throwable, AvroRemoteException {
						try {
							Thread.sleep((Long) ((GenericRecord) requestCapture.getValue()).get("millis"));
						} catch (InterruptedException e) {
							throw new AvroRemoteException(e);
						}
						return null;
					}
				});
		EasyMock.replay(MockSleepyResponder);
		return MockSleepyResponder;
	}

	/** Returns an HTML string. */
	private String generateServletResponse(StatsPlugin statsPlugin) throws IOException {
		StatsServlet servlet;
		try {
			servlet = new StatsServlet(statsPlugin);
		} catch (UnavailableException e1) {
			throw new IOException();
		}
		StringWriter w = new StringWriter();
		try {
			servlet.writeStats(w);
		} catch (Exception e) {
			e.printStackTrace();
		}
		String o = w.toString();
		return o;
	}

	private void makeRequest(Transceiver t) throws Exception {
		GenericRecord params = new GenericData.Record(protocol.getMessages().get("m").getRequest());
		params.put("x", 0);
		GenericRequestor r = new GenericRequestor(protocol, t);
		assertEquals(1, r.request("m", params));
	}

	@Test
	public void testFullServerPath() throws Exception {
		StatsPlugin statsPlugin = new StatsPlugin();
		MockTestResponder.addRPCPlugin(statsPlugin);
		Transceiver t = new LocalTransceiver(MockTestResponder);

		for (int i = 0; i < 10; ++i) {
			makeRequest(t);
		}

		String o = generateServletResponse(statsPlugin);
		assertTrue(o.contains("10 calls"));
	}

	@Test
	public void testMultipleRPCs() throws IOException {
		org.apache.avro.ipc.stats.FakeTicks t = new org.apache.avro.ipc.stats.FakeTicks();
		StatsPlugin statsPlugin = new StatsPlugin(t, StatsPlugin.LATENCY_SEGMENTER, StatsPlugin.PAYLOAD_SEGMENTER);
		RPCContext context1 = makeContext();
		RPCContext context2 = makeContext();
		statsPlugin.serverReceiveRequest(context1);
		t.passTime(100 * MS); // first takes 100ms
		statsPlugin.serverReceiveRequest(context2);
		String r = generateServletResponse(statsPlugin);
		// Check in progress RPCs
		assertTrue(r.contains("m: 0ms"));
		assertTrue(r.contains("m: 100ms"));
		statsPlugin.serverSendResponse(context1);
		t.passTime(900 * MS); // second takes 900ms
		statsPlugin.serverSendResponse(context2);
		r = generateServletResponse(statsPlugin);
		assertTrue(r.contains("Average: 500.0ms"));
	}

	@Test
	public void testPayloadSize() throws Exception {
		StatsPlugin statsPlugin = new StatsPlugin();
		MockTestResponder.addRPCPlugin(statsPlugin);
		Transceiver t = new LocalTransceiver(MockTestResponder);
		makeRequest(t);

		String resp = generateServletResponse(statsPlugin);
		assertTrue(resp.contains("Average: 2.0"));

	}

	private RPCContext makeContext() {
		RPCContext context = new RPCContext();
		context.setMessage(message);
		return context;
	}

	public static void main(String[] args) throws Exception {
		if (args.length == 0) {
			args = new String[] { "7002", "7003" };
		}
		Protocol protocol = Protocol
				.parse("{\"protocol\": \"sleepy\", " + "\"messages\": { \"sleep\": {" + "   \"request\": [{\"name\": \"millis\", \"type\": \"long\"},"
						+ "{\"name\": \"data\", \"type\": \"bytes\"}], " + "   \"response\": \"null\"} } }");
		Responder r = setupSleepyResponder(protocol);
		StatsPlugin p = new StatsPlugin();
		r.addRPCPlugin(p);

		// Start Avro server
		HttpServer avroServer = new HttpServer(r, Integer.parseInt(args[0]));
		avroServer.start();

		StatsServer ss = new StatsServer(p, 8080);

		HttpTransceiver trans = new HttpTransceiver(new URL("http://localhost:" + Integer.parseInt(args[0])));
		GenericRequestor req = new GenericRequestor(protocol, trans);

		while (true) {
			Thread.sleep(1000);
			GenericRecord params = new GenericData.Record(protocol.getMessages().get("sleep").getRequest());
			Random rand = new Random();
			params.put("millis", Math.abs(rand.nextLong()) % 1000);
			int payloadSize = Math.abs(rand.nextInt()) % 10000;
			byte[] payload = new byte[payloadSize];
			rand.nextBytes(payload);
			params.put("data", ByteBuffer.wrap(payload));
			req.request("sleep", params);
		}
	}

}
