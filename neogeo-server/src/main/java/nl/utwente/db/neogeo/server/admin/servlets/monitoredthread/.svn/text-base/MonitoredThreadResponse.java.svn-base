package nl.utwente.db.neogeo.server.admin.servlets.monitoredthread;

import java.net.URI;

import nl.utwente.db.neogeo.server.admin.servlets.monitoredthread.utils.MonitoredThreadUtils;
import nl.utwente.db.neogeo.server.servlets.NeoGeoResponse;
import nl.utwente.db.neogeo.utils.tasks.MonitoredThread;

public class MonitoredThreadResponse extends NeoGeoResponse {
	protected long threadId;
	protected URI statusURI;
	protected URI stopURI;
	protected double progress;
	protected String name;
	protected boolean active;
	
	public void read(MonitoredThread<?> thread) {
		this.setThreadId(thread.getId());
		this.setProgress(thread.getProgress());
		this.setName(thread.getName());
		this.setActive(thread.isAlive());
	}
	
	public long getThreadId() {
		return threadId;
	}

	public void setThreadId(long threadId) {
		this.threadId = threadId;
		
		this.statusURI = MonitoredThreadUtils.createStatusLink(threadId);
		this.stopURI = MonitoredThreadUtils.createStopLink(threadId);
	}

	public URI getStatusURI() {
		return statusURI;
	}

	public void setStatusURI(URI statusURI) {
		this.statusURI = statusURI;
	}

	public URI getStopURI() {
		return stopURI;
	}

	public void setStopURI(URI stopURI) {
		this.stopURI = stopURI;
	}

	public double getProgress() {
		return progress;
	}

	public void setProgress(double progress) {
		this.progress = progress;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	
	public boolean isActive() {
		return active;
	}

	public void setActive(boolean active) {
		this.active = active;
	}
}
