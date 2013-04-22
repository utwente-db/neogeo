package nl.utwente.db.neogeo.utils.parallel;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

public class ParallelProcessorTask<InputType, ResultType> implements Runnable {

	private List<ResultType> results;
	private Method method;
	private Object invokeMethodOn;
	private Object[] parameters;
	private boolean completed = false;
	
	public ParallelProcessorTask(List<ResultType> results, Method method, Object invokeMethodOn, Object[] parameters) {
		this.results = results;
		this.method = method;
		this.invokeMethodOn = invokeMethodOn;
		this.parameters = parameters;
	}

	@SuppressWarnings("unchecked")
	public void run() {
		try {
			results.add((ResultType)method.invoke(invokeMethodOn, parameters));
		} catch (InvocationTargetException e) {
			throw new RuntimeException("InvocationTargetException while executing " + method.getClass() + "." + method.getName(), e);
		} catch (IllegalArgumentException e) {
			throw new RuntimeException("IllegalArgumentException while executing " + method.getClass() + "." + method.getName(), e);
		} catch (IllegalAccessException e) {
			throw new RuntimeException("IllegalAccessException while executing " + method.getClass() + "." + method.getName(), e);
		} finally {
			completed = true;
		}
	}

	public boolean isCompleted() {
		return completed;
	}
}
