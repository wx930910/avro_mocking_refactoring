package org.apache.avro.compiler.schema;

import org.apache.avro.Schema;
import org.apache.avro.SchemaCompatibility;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

public class TestSchemasWithMock {
	private static final String SCHEMA = "{\"type\":\"record\",\"name\":\"SampleNode\",\"doc\":\"caca\","
			+ "\"namespace\":\"org.spf4j.ssdump2.avro\",\n" + " \"fields\":[\n"
			+ "    {\"name\":\"count\",\"type\":\"int\",\"default\":0,\"doc\":\"caca\"},\n"
			+ "    {\"name\":\"kind1\",\"type\":{\"type\":\"enum\", \"name\": \"Kind1\", \"symbols\": [\"A1\", \"B1\"]}},\n"
			+ "    {\"name\":\"kind2\",\"type\":{\"type\":\"enum\", \"name\": \"Kind2\", \"symbols\": [\"A2\", \"B2\"], \"doc\": \"doc\"}},\n"
			+ "    {\"name\":\"pat\",\"type\":{\"type\":\"fixed\", \"name\": \"FixedPattern\", \"size\": 10}},\n"
			+ "    {\"name\":\"uni\",\"type\":[\"int\", \"double\"]},\n" + "    {\"name\":\"mp\",\"type\":{\"type\":\"map\", \"values\": \"int\"}},\n"
			+ "    {\"name\":\"subNodes\",\"type\":\n" + "       {\"type\":\"array\",\"items\":{\n"
			+ "           \"type\":\"record\",\"name\":\"SamplePair\",\n" + "           \"fields\":[\n"
			+ "              {\"name\":\"method\",\"type\":\n" + "                  {\"type\":\"record\",\"name\":\"Method\",\n"
			+ "                  \"fields\":[\n"
			+ "                     {\"name\":\"declaringClass\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"}},\n"
			+ "                     {\"name\":\"methodName\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"}}\n"
			+ "                  ]}},\n" + "              {\"name\":\"node\",\"type\":\"SampleNode\"}]}}}" + "]}";

	private static class MockPrintingVisitor {
		public SchemaVisitor MockedPrintingVisitor;

		public MockPrintingVisitor() {
			this.MockedPrintingVisitor = EasyMock.niceMock(SchemaVisitor.class);
			mockVisitTerminal();
			mockVisitNonTerminal();
			mockAfterVisitNonTerminal();
			mockGet();
			EasyMock.replay(this.MockedPrintingVisitor);
		}

		private void mockVisitTerminal() {
			EasyMock.expect(this.MockedPrintingVisitor.visitTerminal(EasyMock.anyObject(Schema.class))).andAnswer(() -> {
				Schema terminal = EasyMock.getCurrentArgument(0);
				System.out.println("Terminal: " + terminal.getFullName());
				return SchemaVisitorAction.CONTINUE;
			}).anyTimes();
		}

		private void mockVisitNonTerminal() {
			EasyMock.expect(this.MockedPrintingVisitor.visitNonTerminal(EasyMock.anyObject(Schema.class))).andAnswer(() -> {
				Schema terminal = EasyMock.getCurrentArgument(0);
				System.out.println("NONTerminal start: " + terminal.getFullName());
				return SchemaVisitorAction.CONTINUE;
			}).anyTimes();
		}

		private void mockAfterVisitNonTerminal() {
			EasyMock.expect(this.MockedPrintingVisitor.afterVisitNonTerminal(EasyMock.anyObject(Schema.class))).andAnswer(() -> {
				Schema terminal = EasyMock.getCurrentArgument(0);
				System.out.println("NONTerminal end: " + terminal.getFullName());
				return SchemaVisitorAction.CONTINUE;
			}).anyTimes();
		}

		private void mockGet() {
			EasyMock.expect(this.MockedPrintingVisitor.get()).andReturn(null).anyTimes();
		}

	}

	@Test
	public void textCloning() {
		Schema recSchema = new Schema.Parser().parse(SCHEMA);
		Schemas.visit(recSchema, new MockPrintingVisitor().MockedPrintingVisitor);

		CloningVisitor cv = new CloningVisitor(recSchema);
		Schema trimmed = Schemas.visit(recSchema, cv);
		Assert.assertNull(trimmed.getDoc());
		Assert.assertNotNull(recSchema.getDoc());

		SchemaCompatibility.SchemaCompatibilityType compat = SchemaCompatibility.checkReaderWriterCompatibility(trimmed, recSchema).getType();
		Assert.assertEquals(SchemaCompatibility.SchemaCompatibilityType.COMPATIBLE, compat);
		compat = SchemaCompatibility.checkReaderWriterCompatibility(recSchema, trimmed).getType();
		Assert.assertEquals(SchemaCompatibility.SchemaCompatibilityType.COMPATIBLE, compat);
		Assert.assertNotNull(cv.toString());
	}

	@Test
	public void textCloningCopyDocs() {
		Schema recSchema = new Schema.Parser().parse(SCHEMA);
		Schemas.visit(recSchema, new MockPrintingVisitor().MockedPrintingVisitor);

		Schema trimmed = Schemas.visit(recSchema, new CloningVisitor(new CloningVisitor.PropertyCopier() {
			@Override
			public void copy(final Schema first, final Schema second) {
				Schemas.copyLogicalTypes(first, second);
				Schemas.copyAliases(first, second);
			}

			@Override
			public void copy(final Schema.Field first, final Schema.Field second) {
				Schemas.copyAliases(first, second);
			}
		}, true, recSchema));
		Assert.assertEquals("caca", trimmed.getDoc());
		Assert.assertNotNull(recSchema.getDoc());

		SchemaCompatibility.SchemaCompatibilityType compat = SchemaCompatibility.checkReaderWriterCompatibility(trimmed, recSchema).getType();
		Assert.assertEquals(SchemaCompatibility.SchemaCompatibilityType.COMPATIBLE, compat);
		compat = SchemaCompatibility.checkReaderWriterCompatibility(recSchema, trimmed).getType();
		Assert.assertEquals(SchemaCompatibility.SchemaCompatibilityType.COMPATIBLE, compat);
	}

	@Test(expected = IllegalStateException.class)
	public void testCloningError1() {
		// Visit Terminal with union
		Schema recordSchema = new Schema.Parser()
				.parse("{\"type\": \"record\", \"name\": \"R\", \"fields\":[{\"name\": \"f1\", \"type\": [\"int\", \"long\"]}]}");
		new CloningVisitor(recordSchema).visitTerminal(recordSchema.getField("f1").schema());
	}

	@Test(expected = IllegalStateException.class)
	public void testCloningError2() {
		// After visit Non-terminal with int
		Schema recordSchema = new Schema.Parser()
				.parse("{\"type\": \"record\", \"name\": \"R\", \"fields\":[{\"name\": \"f1\", \"type\": \"int\"}]}");
		new CloningVisitor(recordSchema).afterVisitNonTerminal(recordSchema.getField("f1").schema());
	}

	@Test
	public void testHasGeneratedJavaClass() {
		Assert.assertTrue(Schemas.hasGeneratedJavaClass(new Schema.Parser().parse("{\"type\": \"fixed\", \"name\": \"N\", \"size\": 10}")));
		Assert.assertFalse(Schemas.hasGeneratedJavaClass(new Schema.Parser().parse("{\"type\": \"int\"}")));
	}

	@Test
	public void testGetJavaClassName() {
		Assert.assertEquals("N", Schemas.getJavaClassName(new Schema.Parser().parse("{\"type\": \"fixed\", \"name\": \"N\", \"size\": 10}")));
		Assert.assertEquals("N",
				Schemas.getJavaClassName(new Schema.Parser().parse("{\"type\": \"fixed\", \"name\": \"N\", \"size\": 10, \"namespace\": \"\"}")));
		Assert.assertEquals("com.example.N", Schemas
				.getJavaClassName(new Schema.Parser().parse("{\"type\": \"fixed\", \"name\": \"N\", \"size\": 10, \"namespace\": \"com.example\"}")));
	}

	private static class MockTestVisitor {
		SchemaVisitor<String> MockedTestVisitor;
		StringBuilder sb = new StringBuilder();

		public MockTestVisitor() {
			this.MockedTestVisitor = EasyMock.niceMock(SchemaVisitor.class);
			mockVisitTerminal();
			mockVisitNonTerminal();
			mockAfterVisitNonTerminal();
			mockGet();
		}

		public SchemaVisitor<String> replay() {
			EasyMock.replay(this.MockedTestVisitor);
			return this.MockedTestVisitor;
		}

		private void mockVisitTerminal() {
			EasyMock.expect(this.MockedTestVisitor.visitTerminal(EasyMock.anyObject(Schema.class))).andAnswer(() -> {
				Schema terminal = EasyMock.getCurrentArgument(0);
				sb.append(terminal);
				return SchemaVisitorAction.CONTINUE;
			}).anyTimes();
		}

		private void mockVisitNonTerminal() {
			EasyMock.expect(this.MockedTestVisitor.visitNonTerminal(EasyMock.anyObject(Schema.class))).andAnswer(() -> {
				Schema nonTerminal = EasyMock.getCurrentArgument(0);
				String n = nonTerminal.getName();
				sb.append(n).append('.');
				if (n.startsWith("t")) {
					return SchemaVisitorAction.TERMINATE;
				} else if (n.startsWith("ss")) {
					return SchemaVisitorAction.SKIP_SIBLINGS;
				} else if (n.startsWith("st")) {
					return SchemaVisitorAction.SKIP_SUBTREE;
				} else {
					return SchemaVisitorAction.CONTINUE;
				}
			}).anyTimes();
		}

		private void mockAfterVisitNonTerminal() {
			EasyMock.expect(this.MockedTestVisitor.afterVisitNonTerminal(EasyMock.anyObject(Schema.class))).andAnswer(() -> {
				Schema nonTerminal = EasyMock.getCurrentArgument(0);
				sb.append("!");
				String n = nonTerminal.getName();
				if (n.startsWith("ct")) {
					return SchemaVisitorAction.TERMINATE;
				} else if (n.startsWith("css")) {
					return SchemaVisitorAction.SKIP_SIBLINGS;
				} else if (n.startsWith("cst")) {
					return SchemaVisitorAction.SKIP_SUBTREE;
				} else {
					return SchemaVisitorAction.CONTINUE;
				}
			}).anyTimes();
		}

		private void mockGet() {
			EasyMock.expect(this.MockedTestVisitor.get()).andAnswer(() -> {
				return sb.toString();
			}).anyTimes();
		}

	}

	@Test
	public void testVisit1() {
		String s1 = "{\"type\": \"record\", \"name\": \"t1\", \"fields\": [" + "{\"name\": \"f1\", \"type\": \"int\"}" + "]}";
		Assert.assertEquals("t1.", Schemas.visit(new Schema.Parser().parse(s1), new MockTestVisitor().replay()));
	}

	@Test
	public void testVisit2() {
		String s2 = "{\"type\": \"record\", \"name\": \"c1\", \"fields\": [" + "{\"name\": \"f1\", \"type\": \"int\"}" + "]}";
		Assert.assertEquals("c1.\"int\"!", Schemas.visit(new Schema.Parser().parse(s2), new MockTestVisitor().replay()));

	}

	@Test
	public void testVisit3() {
		String s3 = "{\"type\": \"record\", \"name\": \"ss1\", \"fields\": [" + "{\"name\": \"f1\", \"type\": \"int\"}" + "]}";
		Assert.assertEquals("ss1.", Schemas.visit(new Schema.Parser().parse(s3), new MockTestVisitor().replay()));

	}

	@Test
	public void testVisit4() {
		String s4 = "{\"type\": \"record\", \"name\": \"st1\", \"fields\": [" + "{\"name\": \"f1\", \"type\": \"int\"}" + "]}";
		Assert.assertEquals("st1.!", Schemas.visit(new Schema.Parser().parse(s4), new MockTestVisitor().replay()));

	}

	@Test
	public void testVisit5() {
		String s5 = "{\"type\": \"record\", \"name\": \"c1\", \"fields\": ["
				+ "{\"name\": \"f1\", \"type\": {\"type\": \"record\", \"name\": \"c2\", \"fields\": " + "[{\"name\": \"f11\", \"type\": \"int\"}]}},"
				+ "{\"name\": \"f2\", \"type\": \"long\"}" + "]}";
		Assert.assertEquals("c1.c2.\"int\"!\"long\"!", Schemas.visit(new Schema.Parser().parse(s5), new MockTestVisitor().replay()));

	}

	@Test
	public void testVisit6() {
		String s6 = "{\"type\": \"record\", \"name\": \"c1\", \"fields\": ["
				+ "{\"name\": \"f1\", \"type\": {\"type\": \"record\", \"name\": \"ss2\", \"fields\": "
				+ "[{\"name\": \"f11\", \"type\": \"int\"}]}}," + "{\"name\": \"f2\", \"type\": \"long\"}" + "]}";
		Assert.assertEquals("c1.ss2.!", Schemas.visit(new Schema.Parser().parse(s6), new MockTestVisitor().replay()));

	}

	@Test
	public void testVisit7() {
		String s7 = "{\"type\": \"record\", \"name\": \"c1\", \"fields\": ["
				+ "{\"name\": \"f1\", \"type\": {\"type\": \"record\", \"name\": \"css2\", \"fields\": "
				+ "[{\"name\": \"f11\", \"type\": \"int\"}]}}," + "{\"name\": \"f2\", \"type\": \"long\"}" + "]}";
		Assert.assertEquals("c1.css2.\"int\"!!", Schemas.visit(new Schema.Parser().parse(s7), new MockTestVisitor().replay()));
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testVisit8() {
		String s8 = "{\"type\": \"record\", \"name\": \"c1\", \"fields\": ["
				+ "{\"name\": \"f1\", \"type\": {\"type\": \"record\", \"name\": \"cst2\", \"fields\": "
				+ "[{\"name\": \"f11\", \"type\": \"int\"}]}}," + "{\"name\": \"f2\", \"type\": \"int\"}" + "]}";
		Schemas.visit(new Schema.Parser().parse(s8), new MockTestVisitor().replay());
	}

	@Test
	public void testVisit9() {
		String s9 = "{\"type\": \"record\", \"name\": \"c1\", \"fields\": ["
				+ "{\"name\": \"f1\", \"type\": {\"type\": \"record\", \"name\": \"ct2\", \"fields\": "
				+ "[{\"name\": \"f11\", \"type\": \"int\"}]}}," + "{\"name\": \"f2\", \"type\": \"long\"}" + "]}";
		Assert.assertEquals("c1.ct2.\"int\"!", Schemas.visit(new Schema.Parser().parse(s9), new MockTestVisitor().replay()));
	}

	private static class MockTestVisitor10 {
		SchemaVisitor<String> MockedTestVisitor;
		StringBuilder sb = new StringBuilder();

		public MockTestVisitor10() {
			this.MockedTestVisitor = EasyMock.niceMock(SchemaVisitor.class);
			mockVisitTerminal();
			mockVisitNonTerminal();
			mockAfterVisitNonTerminal();
			mockGet();
		}

		public SchemaVisitor<String> replay() {
			EasyMock.replay(this.MockedTestVisitor);
			return this.MockedTestVisitor;
		}

		private void mockVisitTerminal() {
			EasyMock.expect(this.MockedTestVisitor.visitTerminal(EasyMock.anyObject(Schema.class))).andAnswer(() -> {
				return SchemaVisitorAction.SKIP_SUBTREE;
			}).anyTimes();
		}

		private void mockVisitNonTerminal() {
			EasyMock.expect(this.MockedTestVisitor.visitNonTerminal(EasyMock.anyObject(Schema.class))).andAnswer(() -> {
				Schema nonTerminal = EasyMock.getCurrentArgument(0);
				String n = nonTerminal.getName();
				sb.append(n).append('.');
				if (n.startsWith("t")) {
					return SchemaVisitorAction.TERMINATE;
				} else if (n.startsWith("ss")) {
					return SchemaVisitorAction.SKIP_SIBLINGS;
				} else if (n.startsWith("st")) {
					return SchemaVisitorAction.SKIP_SUBTREE;
				} else {
					return SchemaVisitorAction.CONTINUE;
				}
			}).anyTimes();
		}

		private void mockAfterVisitNonTerminal() {
			EasyMock.expect(this.MockedTestVisitor.afterVisitNonTerminal(EasyMock.anyObject(Schema.class))).andAnswer(() -> {
				Schema nonTerminal = EasyMock.getCurrentArgument(0);
				sb.append("!");
				String n = nonTerminal.getName();
				if (n.startsWith("ct")) {
					return SchemaVisitorAction.TERMINATE;
				} else if (n.startsWith("css")) {
					return SchemaVisitorAction.SKIP_SIBLINGS;
				} else if (n.startsWith("cst")) {
					return SchemaVisitorAction.SKIP_SUBTREE;
				} else {
					return SchemaVisitorAction.CONTINUE;
				}
			}).anyTimes();
		}

		private void mockGet() {
			EasyMock.expect(this.MockedTestVisitor.get()).andAnswer(() -> {
				return sb.toString();
			}).anyTimes();
		}

	}

	@Test(expected = UnsupportedOperationException.class)
	public void testVisit10() {
		String s10 = "{\"type\": \"record\", \"name\": \"c1\", \"fields\": ["
				+ "{\"name\": \"f1\", \"type\": {\"type\": \"record\", \"name\": \"ct2\", \"fields\": "
				+ "[{\"name\": \"f11\", \"type\": \"int\"}]}}," + "{\"name\": \"f2\", \"type\": \"int\"}" + "]}";
		Schemas.visit(new Schema.Parser().parse(s10), new MockTestVisitor10().replay());
	}

	private static class MockTestVisitor11 {
		SchemaVisitor<String> MockedTestVisitor;
		StringBuilder sb = new StringBuilder();

		public MockTestVisitor11() {
			this.MockedTestVisitor = EasyMock.niceMock(SchemaVisitor.class);
			mockVisitTerminal();
			mockVisitNonTerminal();
			mockAfterVisitNonTerminal();
			mockGet();
		}

		public SchemaVisitor<String> replay() {
			EasyMock.replay(this.MockedTestVisitor);
			return this.MockedTestVisitor;
		}

		private void mockVisitTerminal() {
			EasyMock.expect(this.MockedTestVisitor.visitTerminal(EasyMock.anyObject(Schema.class))).andAnswer(() -> {
				Schema terminal = EasyMock.getCurrentArgument(0);
				sb.append(terminal).append('.');
				return SchemaVisitorAction.SKIP_SIBLINGS;
			}).anyTimes();
		}

		private void mockVisitNonTerminal() {
			EasyMock.expect(this.MockedTestVisitor.visitNonTerminal(EasyMock.anyObject(Schema.class))).andAnswer(() -> {
				Schema nonTerminal = EasyMock.getCurrentArgument(0);
				String n = nonTerminal.getName();
				sb.append(n).append('.');
				if (n.startsWith("t")) {
					return SchemaVisitorAction.TERMINATE;
				} else if (n.startsWith("ss")) {
					return SchemaVisitorAction.SKIP_SIBLINGS;
				} else if (n.startsWith("st")) {
					return SchemaVisitorAction.SKIP_SUBTREE;
				} else {
					return SchemaVisitorAction.CONTINUE;
				}
			}).anyTimes();
		}

		private void mockAfterVisitNonTerminal() {
			EasyMock.expect(this.MockedTestVisitor.afterVisitNonTerminal(EasyMock.anyObject(Schema.class))).andAnswer(() -> {
				Schema nonTerminal = EasyMock.getCurrentArgument(0);
				sb.append("!");
				String n = nonTerminal.getName();
				if (n.startsWith("ct")) {
					return SchemaVisitorAction.TERMINATE;
				} else if (n.startsWith("css")) {
					return SchemaVisitorAction.SKIP_SIBLINGS;
				} else if (n.startsWith("cst")) {
					return SchemaVisitorAction.SKIP_SUBTREE;
				} else {
					return SchemaVisitorAction.CONTINUE;
				}
			}).anyTimes();
		}

		private void mockGet() {
			EasyMock.expect(this.MockedTestVisitor.get()).andAnswer(() -> {
				return sb.toString();
			}).anyTimes();
		}

	}

	@Test
	public void testVisit11() {
		String s11 = "{\"type\": \"record\", \"name\": \"c1\", \"fields\": ["
				+ "{\"name\": \"f1\", \"type\": {\"type\": \"record\", \"name\": \"c2\", \"fields\": "
				+ "[{\"name\": \"f11\", \"type\": \"int\"},{\"name\": \"f12\", \"type\": \"double\"}" + "]}},"
				+ "{\"name\": \"f2\", \"type\": \"long\"}" + "]}";
		Assert.assertEquals("c1.c2.\"int\".!\"long\".!", Schemas.visit(new Schema.Parser().parse(s11), new MockTestVisitor11().replay()));
	}

	private static class MockTestVisitor12 {
		SchemaVisitor<String> MockedTestVisitor;
		StringBuilder sb = new StringBuilder();

		public MockTestVisitor12() {
			this.MockedTestVisitor = EasyMock.niceMock(SchemaVisitor.class);
			mockVisitTerminal();
			mockVisitNonTerminal();
			mockAfterVisitNonTerminal();
			mockGet();
		}

		public SchemaVisitor<String> replay() {
			EasyMock.replay(this.MockedTestVisitor);
			return this.MockedTestVisitor;
		}

		private void mockVisitTerminal() {
			EasyMock.expect(this.MockedTestVisitor.visitTerminal(EasyMock.anyObject(Schema.class))).andAnswer(() -> {
				Schema terminal = EasyMock.getCurrentArgument(0);
				sb.append(terminal).append('.');
				return SchemaVisitorAction.TERMINATE;
			}).anyTimes();
		}

		private void mockVisitNonTerminal() {
			EasyMock.expect(this.MockedTestVisitor.visitNonTerminal(EasyMock.anyObject(Schema.class))).andAnswer(() -> {
				Schema nonTerminal = EasyMock.getCurrentArgument(0);
				String n = nonTerminal.getName();
				sb.append(n).append('.');
				if (n.startsWith("t")) {
					return SchemaVisitorAction.TERMINATE;
				} else if (n.startsWith("ss")) {
					return SchemaVisitorAction.SKIP_SIBLINGS;
				} else if (n.startsWith("st")) {
					return SchemaVisitorAction.SKIP_SUBTREE;
				} else {
					return SchemaVisitorAction.CONTINUE;
				}
			}).anyTimes();
		}

		private void mockAfterVisitNonTerminal() {
			EasyMock.expect(this.MockedTestVisitor.afterVisitNonTerminal(EasyMock.anyObject(Schema.class))).andAnswer(() -> {
				Schema nonTerminal = EasyMock.getCurrentArgument(0);
				sb.append("!");
				String n = nonTerminal.getName();
				if (n.startsWith("ct")) {
					return SchemaVisitorAction.TERMINATE;
				} else if (n.startsWith("css")) {
					return SchemaVisitorAction.SKIP_SIBLINGS;
				} else if (n.startsWith("cst")) {
					return SchemaVisitorAction.SKIP_SUBTREE;
				} else {
					return SchemaVisitorAction.CONTINUE;
				}
			}).anyTimes();
		}

		private void mockGet() {
			EasyMock.expect(this.MockedTestVisitor.get()).andAnswer(() -> {
				return sb.toString();
			}).anyTimes();
		}

	}

	@Test
	public void testVisit12() {
		String s12 = "{\"type\": \"record\", \"name\": \"c1\", \"fields\": ["
				+ "{\"name\": \"f1\", \"type\": {\"type\": \"record\", \"name\": \"ct2\", \"fields\": "
				+ "[{\"name\": \"f11\", \"type\": \"int\"}]}}," + "{\"name\": \"f2\", \"type\": \"long\"}" + "]}";
		Assert.assertEquals("c1.ct2.\"int\".", Schemas.visit(new Schema.Parser().parse(s12), new MockTestVisitor12().replay()));
	}

	private static class MockTestVisitor13 {
		SchemaVisitor<String> MockedTestVisitor;
		StringBuilder sb = new StringBuilder();

		public MockTestVisitor13() {
			this.MockedTestVisitor = EasyMock.niceMock(SchemaVisitor.class);
			mockVisitTerminal();
			mockVisitNonTerminal();
			mockAfterVisitNonTerminal();
			mockGet();
		}

		public SchemaVisitor<String> replay() {
			EasyMock.replay(this.MockedTestVisitor);
			return this.MockedTestVisitor;
		}

		private void mockVisitTerminal() {
			EasyMock.expect(this.MockedTestVisitor.visitTerminal(EasyMock.anyObject(Schema.class))).andAnswer(() -> {
				Schema terminal = EasyMock.getCurrentArgument(0);
				sb.append(terminal).append('.');
				return SchemaVisitorAction.SKIP_SIBLINGS;
			}).anyTimes();
		}

		private void mockVisitNonTerminal() {
			EasyMock.expect(this.MockedTestVisitor.visitNonTerminal(EasyMock.anyObject(Schema.class))).andAnswer(() -> {
				Schema nonTerminal = EasyMock.getCurrentArgument(0);
				String n = nonTerminal.getName();
				sb.append(n).append('.');
				if (n.startsWith("t")) {
					return SchemaVisitorAction.TERMINATE;
				} else if (n.startsWith("ss")) {
					return SchemaVisitorAction.SKIP_SIBLINGS;
				} else if (n.startsWith("st")) {
					return SchemaVisitorAction.SKIP_SUBTREE;
				} else {
					return SchemaVisitorAction.CONTINUE;
				}
			}).anyTimes();
		}

		private void mockAfterVisitNonTerminal() {
			EasyMock.expect(this.MockedTestVisitor.afterVisitNonTerminal(EasyMock.anyObject(Schema.class))).andAnswer(() -> {
				Schema nonTerminal = EasyMock.getCurrentArgument(0);
				sb.append("!");
				String n = nonTerminal.getName();
				if (n.startsWith("ct")) {
					return SchemaVisitorAction.TERMINATE;
				} else if (n.startsWith("css")) {
					return SchemaVisitorAction.SKIP_SIBLINGS;
				} else if (n.startsWith("cst")) {
					return SchemaVisitorAction.SKIP_SUBTREE;
				} else {
					return SchemaVisitorAction.CONTINUE;
				}
			}).anyTimes();
		}

		private void mockGet() {
			EasyMock.expect(this.MockedTestVisitor.get()).andAnswer(() -> {
				return sb.toString();
			}).anyTimes();
		}

	}

	@Test
	public void testVisit13() {
		String s12 = "{\"type\": \"int\"}";
		Assert.assertEquals("\"int\".", Schemas.visit(new Schema.Parser().parse(s12), new MockTestVisitor13().replay()));
	}
}
