package com.project.rithomas.jobexecution.tnp;

import static org.powermock.api.mockito.PowerMockito.mockStatic;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.project.rithomas.jobexecution.common.boundary.CalculateLBUB;
import com.project.rithomas.jobexecution.common.exception.JobExecutionException;
import com.project.rithomas.jobexecution.common.util.DateFunctionTransformation;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.jobexecution.common.util.QueryExecutor;
import com.project.rithomas.sdk.model.meta.Boundary;
import com.project.rithomas.sdk.model.meta.query.BoundaryQuery;
import com.project.rithomas.sdk.workflow.WorkFlowContext;
import com.project.rithomas.sdk.workflow.WorkFlowExecutionException;


@RunWith(PowerMockRunner.class)
@PrepareForTest({ TNPLBUBCalculation.class, DateFunctionTransformation.class,
		CalculateLBUB.class, TNPLBUBCalculationHelper.class })
public class TNPLBUBCalculationTest {

	private static final String SOURCE_TABLE_NAME = "tnp_threshold_cache_hbt";

	private static final String HIVE_PART_COL = "mockBoundary_HIVE_PARTITION_COLUMN";

	private static final String RET_PROP = "dummyJob_RETENTIONDAYS";

	private static final String HIVE_COLUMN = "city";

	private static final String MOCK_PLEVEL = "minutes";

	private static final String RET_DAYS = "2";

	private static final String JOB_TYPE = "Import";

	private static final String MOCK_SOURCE = "source";

	private static final String MOCK_PART_COLUMN = "sourcepartcolumn";

	private List<String> jobList;

	private TNPLBUBCalculation calculation;

	private List<String> minSrcDateValList;

	private Long truncTime = System.nanoTime();

	Date date = new Date();

	String[] mockDateSet = { date.toString() };

	WorkFlowContext context = null;

	@Mock
	WorkFlowContext mockContext;

	@Mock
	BoundaryQuery mockBoundaryQuery;

	@Mock
	DateFunctionTransformation mockDFT;

	private List<Boundary> mockSrcBoundaryList;

	@Mock
	Boundary mockBoundary;

	@Mock
	CalculateLBUB mockCalculateLBUB;

	@Mock
	QueryExecutor mockQueryExecutor;

	@Before
	public void setUp() throws Exception {
		MockitoAnnotations.initMocks(this);
		this.jobList = new ArrayList<String>();
		this.jobList.add("dummyJob");
		this.mockSrcBoundaryList = new ArrayList<Boundary>();
		this.mockSrcBoundaryList.add(mockBoundary);
		this.mockSrcBoundaryList.add(mockBoundary);
		this.minSrcDateValList = new ArrayList<String>();
		this.minSrcDateValList.add("1");
		this.calculation = new TNPLBUBCalculation();
		this.context = new JobExecutionContext();
		PowerMockito.whenNew(QueryExecutor.class).withNoArguments()
				.thenReturn(this.mockQueryExecutor);
		PowerMockito.whenNew(BoundaryQuery.class).withNoArguments()
				.thenReturn(mockBoundaryQuery);
		PowerMockito.whenNew(QueryExecutor.class).withNoArguments()
				.thenReturn(this.mockQueryExecutor);
	}

	@Test
	public void testExecute() throws WorkFlowExecutionException {
		mockContextProp();
		this.calculation.execute(mockContext);
		Mockito.verify(mockBoundaryQuery).retrieveByJobId(
				Mockito.anyString());
	}

	@Test
	public void testExecuteWhenMaxValueIsNull()
			throws WorkFlowExecutionException, JobExecutionException {
		mockContextProp();
		Mockito.when(
				mockContext.getProperty(JobExecutionContext.MAXVALUE))
				.thenReturn(null);
		Mockito.when(mockContext.getProperty(RET_PROP)).thenReturn(
				RET_DAYS);
		Mockito.when(mockContext.getProperty(HIVE_PART_COL))
				.thenReturn(HIVE_COLUMN);
		Mockito.when(
				mockContext
						.getProperty(JobExecutionContext.SOURCE_TABLE_NAME))
				.thenReturn(SOURCE_TABLE_NAME);
		// PowerMockito.when(
		// this.mockQueryExecutor.executeHiveQuery(Matchers.anyString()))
		// .thenReturn(this.minSrcDateValList);
		this.calculation.execute(mockContext);
		Mockito.verify(mockBoundaryQuery).retrieveByJobId(
				Mockito.anyString());
		// Mockito.verify(this.mockQueryExecutor).executeHiveQuery(
		// Matchers.anyString());
	}

	@Test
	public void testExecuteWhenMaxValueIsNullWithImport()
			throws JobExecutionException, WorkFlowExecutionException {
		mockContextProp();
		Mockito.when(
				mockContext.getProperty(JobExecutionContext.MAXVALUE))
				.thenReturn(null);
		Mockito.when(mockContext.getProperty(RET_PROP)).thenReturn(
				RET_DAYS);
		PowerMockito.when(
				mockContext.getProperty(JobExecutionContext.JOBTYPE))
				.thenReturn(JOB_TYPE);
		this.calculation.execute(mockContext);
		Mockito.verify(mockBoundaryQuery).retrieveByJobId(
				Mockito.anyString());
	}

	private void mockContextProp() {
		try {
			Mockito.when(
					mockContext
							.getProperty(JobExecutionContext.RETENTIONDAYS))
					.thenReturn(RET_DAYS);
			Mockito.when(
					mockContext.getProperty(JobExecutionContext.MAXVALUE))
					.thenReturn(new Timestamp(System.currentTimeMillis()));
			Mockito.when(
					mockContext
							.getProperty(JobExecutionContext.SOURCEJOBLIST))
					.thenReturn(this.jobList);
			Mockito
					.when(mockContext
							.getProperty(JobExecutionContext.AGG_RETENTIONDAYS))
					.thenReturn(Integer.valueOf(RET_DAYS));
			Mockito
					.when(mockContext
							.getProperty(JobExecutionContext.SOURCE_BOUND_LIST))
					.thenReturn(this.mockSrcBoundaryList);
			Mockito.when(
					mockContext.getProperty(JobExecutionContext.PLEVEL))
					.thenReturn(MOCK_PLEVEL);
			Mockito.when(
					mockContext.getProperty(JobExecutionContext.LB))
					.thenReturn(System.currentTimeMillis());
			Mockito.when(
					mockContext.getProperty(JobExecutionContext.SOURCE))
					.thenReturn(MOCK_SOURCE);
			Mockito.when(
					mockContext
							.getProperty(JobExecutionContext.SOURCEPARTCOLUMN))
					.thenReturn(MOCK_PART_COLUMN);
			Mockito.when(mockContext.getProperty(JobExecutionContext.WEEK_START_DAY)).thenReturn("1");
			Mockito.when(mockBoundary.getMaxValue()).thenReturn(
					new Timestamp(System.currentTimeMillis()));
			Mockito.when(mockBoundary.getJobId()).thenReturn("jobId");
			Mockito.when(mockContext.getProperty("jobId_PLEVEL")).thenReturn("plevel");
			mockStatic(DateFunctionTransformation.class);
			PowerMockito.when(DateFunctionTransformation.getInstance())
					.thenReturn(mockDFT);
			Mockito.when(
					mockDFT.getNextBound(Mockito.anyLong(),
							Mockito.anyString())).thenReturn(
					Calendar.getInstance());
			Mockito.when(
					mockDFT.getTrunc(Mockito.anyLong(),
							Mockito.anyString(), Mockito.anyString())).thenReturn(truncTime);

			PowerMockito.when(mockDFT.getDate(Mockito.anyString()))
					.thenReturn(new Date());
			PowerMockito
					.when(mockBoundaryQuery.retrieveByJobId(Mockito
							.anyString())).thenReturn(this.mockSrcBoundaryList);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
