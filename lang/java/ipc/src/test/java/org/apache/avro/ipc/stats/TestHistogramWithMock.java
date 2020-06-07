package org.apache.avro.ipc.stats;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

import org.apache.avro.ipc.stats.Histogram.Entry;
import org.apache.avro.ipc.stats.Histogram.Segmenter;
import org.easymock.EasyMock;
import org.junit.Test;

public class TestHistogramWithMock {

	/**
	 * Mock {@link TestHistogram#SingleBucketSegmenter }
	 * 
	 * @return
	 */
	private static Segmenter<String, Float> setupMockSingleBucketSegmenter() {
		Segmenter<String, Float> MockSingleBucketSegmenter = EasyMock.niceMock(Segmenter.class);
		// Mock getBuckets()
		EasyMock.expect(MockSingleBucketSegmenter.getBuckets()).andReturn(Collections.singletonList("X").iterator());
		// Mock getBoundaryLabels()
		EasyMock.expect(MockSingleBucketSegmenter.getBoundaryLabels()).andReturn(Collections.singletonList("X"));
		// Mock getBucketLabels()
		EasyMock.expect(MockSingleBucketSegmenter.getBucketLabels()).andReturn(Collections.singletonList("X"));
		// Mock segment(Float)
		EasyMock.expect(MockSingleBucketSegmenter.segment(EasyMock.anyObject(Float.class))).andReturn(0);
		// Mock size()
		EasyMock.expect(MockSingleBucketSegmenter.size()).andReturn(1);

		EasyMock.replay(MockSingleBucketSegmenter);
		return MockSingleBucketSegmenter;
	}

	@Test
	public void testBasicOperation() {
		Segmenter<String, Integer> s = new Histogram.TreeMapSegmenter<>(new TreeSet<>(Arrays.asList(0, 1, 2, 4, 8, 16)));

		Histogram<String, Integer> h = new Histogram<>(s);

		for (int i = 0; i < 20; ++i) {
			h.add(i);
		}
		assertEquals(20, h.getCount());
		assertArrayEquals(new int[] { 1, 1, 2, 4, 8, 4 }, h.getHistogram());

		assertEquals("[0,1)=1;[1,2)=1;[2,4)=2;[4,8)=4;[8,16)=8;[16,infinity)=4", h.toString());

		String[] correctBucketLabels = { "[0,1)", "[1,2)", "[2,4)", "[4,8)", "[8,16)", "[16,infinity)" };

		// test bucket iterator
		int pos = 0;
		Iterator<String> it = h.getSegmenter().getBuckets();
		while (it.hasNext()) {
			assertEquals(correctBucketLabels[pos], it.next());
			pos = pos + 1;
		}
		assertEquals(correctBucketLabels.length, pos);

		List<String> labels = h.getSegmenter().getBucketLabels();
		assertEquals(correctBucketLabels.length, labels.size());
		if (labels.size() == correctBucketLabels.length) {
			for (int i = 0; i < labels.size(); i++) {
				assertEquals(correctBucketLabels[i], labels.get(i));
			}
		}

		String[] correctBoundryLabels = { "0", "1", "2", "4", "8", "16" };
		List<String> boundryLabels = h.getSegmenter().getBoundaryLabels();

		assertEquals(correctBoundryLabels.length, boundryLabels.size());
		if (boundryLabels.size() == correctBoundryLabels.length) {
			for (int i = 0; i < boundryLabels.size(); i++) {
				assertEquals(correctBoundryLabels[i], boundryLabels.get(i));
			}
		}

		List<Entry<String>> entries = new ArrayList<>();
		for (Entry<String> entry : h.entries()) {
			entries.add(entry);
		}
		assertEquals("[0,1)", entries.get(0).bucket);
		assertEquals(4, entries.get(5).count);
		assertEquals(6, entries.size());

		h.add(1010);
		h.add(9191);
		List<Integer> recent = h.getRecentAdditions();
		assertTrue(recent.contains(1010));
		assertTrue(recent.contains(9191));

	}

	@Test(expected = Histogram.SegmenterException.class)
	public void testBadValue() {
		Segmenter<String, Long> s = new Histogram.TreeMapSegmenter<>(new TreeSet<>(Arrays.asList(0L, 1L, 2L, 4L, 8L, 16L)));

		Histogram<String, Long> h = new Histogram<>(s);
		h.add(-1L);
	}

	@Test
	public void testFloatHistogram() {
		FloatHistogram<String> h = new FloatHistogram<>(setupMockSingleBucketSegmenter());
		h.add(12.0f);
		h.add(10.0f);
		h.add(20.0f);

		assertEquals(3, h.getCount());
		assertEquals(14.0f, h.getMean(), 0.0001);
		assertEquals(5.291f, h.getUnbiasedStdDev(), 0.001);
	}
}
