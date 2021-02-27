//
// package com.project.rithomas.jobexecution.common.util;
//
// import static org.junit.Assert.assertEquals;
// import static org.junit.Assert.assertNotNull;
//
// import java.util.ArrayList;
// import java.util.List;
//
// import org.junit.Test;
//
// public class JobTest {
//
// @Test
// public void testSetGetNameAndQueryHints() {
//
// Job job = new Job();
// job.setName("Usage_SMS_1_LoadJob");
// List<String> hints = new ArrayList<String>();
// hints.add("TestHint1");
// hints.add("TestHint2");
// QueryHints queryHints = new QueryHints();
// queryHints.setHint(hints);
// job.setQueryHints(queryHints);
//
// assertEquals("Usage_SMS_1_LoadJob", job.getName());
// assertNotNull(job.getQueryHints());
// assertEquals(2, job.getQueryHints().getHint().size());
// }
//
// }
