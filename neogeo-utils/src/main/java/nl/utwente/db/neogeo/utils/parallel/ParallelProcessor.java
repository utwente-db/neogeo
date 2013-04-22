package nl.utwente.db.neogeo.utils.parallel;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class ParallelProcessor<InputType, ResultType> {
	public final static String INPUT_OBJECT = "inputObject";
	
	public List<ResultType> run(Collection<InputType> inputs, String methodName, Object[] parameters) throws NoSuchMethodException {
		return run(inputs, methodName, parameters, null);
	}

	/**
	 * 
	 * @param inputs
	 * @param methodName
	 * @param parameters ParallelProcessor.INPUT_OBJECT can be used to refer to the input object.
	 * @param invokeMethodOn
	 * @return
	 * @throws NoSuchMethodException
	 */
	public List<ResultType> run(Collection<InputType> inputs, String methodName, Object[] parameters, Object invokeMethodOn) throws NoSuchMethodException {
		if (parameters == null) {
			parameters = new Object[]{};
		}
		
		List<ResultType> results = new ArrayList<ResultType>();
		List<ParallelProcessorTask<InputType, ResultType>> tasks = new ArrayList<ParallelProcessorTask<InputType,ResultType>>();
		
		for (InputType input : inputs) {
			if (invokeMethodOn == null) {
				invokeMethodOn = input;
			}
			
			Object[] filledInParameters = parameters.clone();
			
			for (int i = 0; i < parameters.length; i++) {
				if (INPUT_OBJECT.equals(parameters[i])) {
					filledInParameters[i] = input;
				}
			}

			Class<Object>[] parameterTypes = getTypes(filledInParameters);
			Method method = invokeMethodOn.getClass().getMethod(methodName, parameterTypes);
			
			ParallelProcessorTask<InputType, ResultType> task = new ParallelProcessorTask<InputType, ResultType>(results, method, invokeMethodOn, filledInParameters);
			
			run(task);
			tasks.add(task);
		}
		
		while (!allCompleted(tasks)) {
			try {
				Thread.sleep(25);
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
		}
		
		return results;
	}
	
	public boolean allCompleted(Collection<ParallelProcessorTask<InputType, ResultType>> tasks) {
		for (ParallelProcessorTask<InputType, ResultType> task : tasks) {
			if (!task.isCompleted()) {
				return false;
			}
		}
		
		return true;
	}
	
	public void run(Collection<Runnable> tasks) {
		for (Runnable task : tasks) {
			run(task);
		}
	}
	
	public void run(Runnable task) {
		Thread thread = new Thread(task);
		thread.start();
	}
	
	@SuppressWarnings("unchecked")
	protected Class<Object>[] getTypes(Object[] variables) {
		if (variables == null) {
			return null;
		}
		
		Class<Object>[] result = new Class[variables.length];
		
		for (int i = 0; i < variables.length; i++) {
			result[i] = (Class<Object>)variables[i].getClass();
		}
		
		return result;
	}
}
