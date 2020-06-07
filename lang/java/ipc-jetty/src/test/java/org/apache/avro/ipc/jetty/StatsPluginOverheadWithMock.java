package org.apache.avro.ipc.jetty;

import java.io.IOException;
import java.net.URL;

import org.apache.avro.AvroRemoteException;
import org.apache.avro.Protocol;
import org.apache.avro.Protocol.Message;
import org.apache.avro.ipc.HttpTransceiver;
import org.apache.avro.ipc.Responder;
import org.apache.avro.ipc.Transceiver;
import org.apache.avro.ipc.generic.GenericRequestor;
import org.apache.avro.ipc.generic.GenericResponder;
import org.apache.avro.ipc.stats.StatsPlugin;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.easymock.IAnswer;

public class StatsPluginOverheadWithMock {
	/** Number of RPCs per iteration. */
	private static final int COUNT = 100000;
	private static final Protocol NULL_PROTOCOL = Protocol
			.parse("{\"protocol\": \"null\", " + "\"messages\": { \"null\": {" + "   \"request\": [], " + "   \"response\": \"null\"} } }");

	private static GenericResponder MockGenericResponder(Protocol local) {
		GenericResponder MockedGenericResponder = EasyMock.partialMockBuilder(GenericResponder.class).withConstructor(Protocol.class).withArgs(local)
				.createMock();
		Capture<Object> captureMethodRespondRequestArg = EasyMock.newCapture();
		try {
			EasyMock.expect(MockedGenericResponder.respond(EasyMock.anyObject(Message.class), EasyMock.capture(captureMethodRespondRequestArg)))
					.andAnswer(new IAnswer<Object>() {
						@Override
						public Object answer() throws AvroRemoteException {
							return captureMethodRespondRequestArg.getValue();
						}
					}).anyTimes();
		} catch (Exception e) {
			e.printStackTrace();
		}
		EasyMock.replay(MockedGenericResponder);
		return MockedGenericResponder;
	}

	public static void main(String[] args) throws Exception {
		double with = sendRpcs(true) / 1000000000.0;
		double without = sendRpcs(false) / 1000000000.0;

		System.out.println(String.format("Overhead: %f%%.  RPC/s: %f (with) vs %f (without).  " + "RPC time (ms): %f vs %f",
				100 * (with - without) / (without), COUNT / with, COUNT / without, 1000 * with / COUNT, 1000 * without / COUNT));
	}

	/** Sends RPCs and returns nanos elapsed. */
	private static long sendRpcs(boolean withPlugin) throws Exception {
		HttpServer server = createServer(withPlugin);
		Transceiver t = new HttpTransceiver(new URL("http://127.0.0.1:" + server.getPort() + "/"));
		GenericRequestor requestor = new GenericRequestor(NULL_PROTOCOL, t);

		long now = System.nanoTime();
		for (int i = 0; i < COUNT; ++i) {
			requestor.request("null", null);
		}
		long elapsed = System.nanoTime() - now;
		t.close();
		server.close();
		return elapsed;
	}

	/** Starts an Avro server. */
	private static HttpServer createServer(boolean withPlugin) throws IOException {
		Responder r = MockGenericResponder(NULL_PROTOCOL);
		if (withPlugin) {
			r.addRPCPlugin(new StatsPlugin());
		}
		// Start Avro server
		HttpServer server = new HttpServer(r, 0);
		server.start();
		return server;
	}
}
