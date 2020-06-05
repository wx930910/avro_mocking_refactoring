package org.apache.avro.ipc.stats;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.avro.ipc.stats.Stopwatch.Ticks;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.junit.Before;
import org.junit.Test;

/**
 * Implement Mocking Framework to avoid creating #FakeTicks class.
 * 
 * @author Twilight
 *
 */
public class TestMockStopWatch {
	private Ticks ticks;
	private long time = 0;

	@Before
	public void setUp() throws Exception {
		ticks = EasyMock.createNiceMock(Ticks.class);
		EasyMock.expect(ticks.ticks()).andAnswer(new IAnswer<Long>() {
			@Override
			public Long answer() throws Throwable {
				// System.out.println("Time: " + time);
				return time;
			}
		}).anyTimes();
		EasyMock.replay(ticks);
	}

	private void passTime(Ticks tick, long time) {

	}

	@Test
	public void testNormalWithMock() {
		Stopwatch s = new Stopwatch(ticks);
		this.time += 10;
		s.start();
		this.time += 20;
		ticks.ticks();
		assertEquals(20, s.elapsedNanos());
		ticks.ticks();
		this.time += 40;
		s.stop();
		this.time += 80;
		assertEquals(60, s.elapsedNanos());
		EasyMock.verify(this.ticks);
	}

	@Test(expected = IllegalStateException.class)
	public void testNotStarted1() {
		Stopwatch s = new Stopwatch(ticks);
		s.elapsedNanos();
	}

	@Test(expected = IllegalStateException.class)
	public void testNotStarted2() {
		Stopwatch s = new Stopwatch(ticks);
		s.stop();
	}

	@Test(expected = IllegalStateException.class)
	public void testTwiceStarted() {
		Stopwatch s = new Stopwatch(ticks);
		s.start();
		s.start();
	}

	@Test(expected = IllegalStateException.class)
	public void testTwiceStopped() {
		Stopwatch s = new Stopwatch(ticks);
		s.start();
		s.stop();
		s.stop();
	}

	@Test
	public void testSystemStopwatch() {
		Stopwatch s = new Stopwatch(Stopwatch.SYSTEM_TICKS);
		s.start();
		s.stop();
		assertTrue(s.elapsedNanos() >= 0);
	}

}
