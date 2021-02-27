package com.project.rithomas.jobexecution.common.util;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;

public class ProcessUtil {
	
	private final static AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(ProcessUtil.class);
	
	public static Integer getExitValue(Process process)
			throws InterruptedException {
		return getExitValue(process, -1);
	}

	public static Integer getExitValue(Process process, int timeout)
			throws InterruptedException {
		Integer exitValue = -1;
		if (timeout == -1) {
			exitValue = process.waitFor();
		} else {
			Integer waitTime = 0;
			// Sleep for 1 second and check if the process has exited, if not
			// continue this in loop for a total of 2 minutes time-out. Else
			// return
			// -1
			do {
				Thread.sleep(1000L);
				try {
					exitValue = process.exitValue();
					break;
				} catch (IllegalThreadStateException ex) {
					LOGGER.debug(
							"Message while getting process exit value: {}. Waiting for the process to exit..",
							ex.getMessage());
				}
				waitTime++;
			} while (waitTime < timeout);
		}
		return exitValue;
	}
	
	public static List<String> getConsoleMessages(Process process)
			throws IOException {
		return getConsoleOutput(process, false);
	}

	public static List<String> getConsoleError(Process process)
			throws IOException {
		return getConsoleOutput(process, true);
	}

	public static List<String> getConsoleOutput(Process process,
			boolean errorStream) throws IOException {
		InputStream is = null;
		InputStreamReader isr = null;
		BufferedReader br = null;
		List<String> lines = new ArrayList<>();
		try {
			if (errorStream) {
				is = process.getErrorStream();
			} else {
				is = process.getInputStream();
			}
			isr = new InputStreamReader(is);
			br = new BufferedReader(isr);
			String line;
			while ((line = br.readLine()) != null) {
				lines.add(line);
			}
		} finally {
				if (br != null) {
					close(br);
					close(isr);
					close(is);
				}
		}
		return lines;
	}

	private static void close(Closeable c) throws IOException {
		try {
			if (c != null) {
				c.close();
			}
		} catch (IOException ex) {
			LOGGER.warn("Error closing stream {}", ex.getMessage());
		}
	}


}
