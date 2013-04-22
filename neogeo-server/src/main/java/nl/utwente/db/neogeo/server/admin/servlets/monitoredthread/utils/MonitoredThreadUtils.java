package nl.utwente.db.neogeo.server.admin.servlets.monitoredthread.utils;

import java.net.URI;
import java.net.URISyntaxException;

import nl.utwente.db.neogeo.core.NeoGeoException;
import nl.utwente.db.neogeo.utils.tasks.MonitoredThread;

public class MonitoredThreadUtils {
	public static String STATUS_URI_PREFIX = "monitored-thread/status?key=secretKey&id=";
	public static String STOP_URI_PREFIX = "monitored-thread/stop?key=secretKey&id=";

	public static URI createStatusLink(long monitoredThreadId) {
		return createURI(STATUS_URI_PREFIX + monitoredThreadId);
	}
	
	public static URI createStatusLink(MonitoredThread<?> monitoredThread) {
		return createStatusLink(monitoredThread.getId());
	}

	
	public static URI createStopLink(long monitoredThreadId) {
		return createURI(STOP_URI_PREFIX + monitoredThreadId);
	}
	
	public static URI createStopLink(MonitoredThread<?> monitoredThread) {
		return createStopLink(monitoredThread.getId());
	}
	
	public static URI createURI(String uri) {
		try {
			return new URI(uri);
		} catch (URISyntaxException e) {
			throw new NeoGeoException("Unexpected error while trying to create a uri", e);
		}
	}
}