package nl.utwente.db.neogeo.server.admin;

import nl.utwente.db.neogeo.utils.tasks.MonitoredRunnable;
import nl.utwente.db.neogeo.utils.test.TestResult;

import org.apache.log4j.Logger;

public class DummyTestRunnable extends MonitoredRunnable<TestResult> {

	private Logger logger = Logger.getLogger(DummyTestRunnable.class);
	private boolean stopped = false;
	
	private int nrTestsExecuted = 0;
	private int nrTestsToExecute = 10;

	public void run() {
		for (int i = 1; i <= nrTestsToExecute; i++) {
			if (this.stopped ) {
				break;
			}
			
			TestResult testResult = new TestResult();
		
			testResult.setName("Test " + i);
			testResult.setSuccess(Math.random() > 0.2);
			testResult.setDescription("Test " + i + " completed");
			
			addUpdate(testResult);
			logger.debug("Added update " + testResult);
			
			nrTestsExecuted++;
			
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	@Override
	public void stop() {
		this.stopped = true;
	}
	
	@Override
	public double getProgress() {
		return ((double)nrTestsExecuted / nrTestsToExecute);
	}
}
