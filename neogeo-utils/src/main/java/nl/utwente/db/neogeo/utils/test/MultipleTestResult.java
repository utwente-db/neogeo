package nl.utwente.db.neogeo.utils.test;

import java.util.ArrayList;
import java.util.List;

public class MultipleTestResult extends TestResult {
	protected List<TestResult> results = new ArrayList<TestResult>();

	public List<TestResult> getResults() {
		return results;
	}

	public void setResults(List<TestResult> results) {
		this.results = results;
	}
	
	public void addResult(TestResult result) {
		results.add(result);
	}

	@Override
	public String toString() {
		return this.getClass().getSimpleName() + " [results=" + results + ", name=" + name
				+ ", success=" + success + ", description=" + description + "]";
	}
}
