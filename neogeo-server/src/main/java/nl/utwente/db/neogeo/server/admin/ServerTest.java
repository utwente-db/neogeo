package nl.utwente.db.neogeo.server.admin;

import nl.utwente.db.neogeo.utils.tasks.MonitoredThread;

public interface ServerTest {
	public MonitoredThread<?> test();
}
