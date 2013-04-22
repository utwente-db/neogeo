package nl.utwente.db.neogeo.db.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import nl.utwente.db.neogeo.core.NeoGeoException;
import nl.utwente.db.neogeo.core.model.ModelObject;
import nl.utwente.db.neogeo.db.hibernate.BaseModelObjectDAO;
import nl.utwente.db.neogeo.utils.FileUtils;

import org.apache.log4j.Logger;
import org.hibernate.FlushMode;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.cfg.Configuration;
import org.hibernate.jdbc.Work;
import org.hibernate.service.ServiceRegistry;
import org.hibernate.service.ServiceRegistryBuilder;
import org.springframework.beans.BeanWrapperImpl;

public abstract class HibernateUtils {
	
	private static final Configuration   CONFIGURATION    = new Configuration().configure(FileUtils.getFileFromClassPath("conf/hibernate.cfg.xml"));
	private static final ServiceRegistry SERVICE_REGISTRY = new ServiceRegistryBuilder().applySettings(CONFIGURATION.getProperties()).buildServiceRegistry();
	private static final SessionFactory  SESSION_FACTORY  = CONFIGURATION.buildSessionFactory(SERVICE_REGISTRY);
	
	static {
		Logger.getLogger(HibernateUtils.class).info(SESSION_FACTORY);
	}
	
	public static Transaction getTransaction() {
		Session session = getSession();
		Transaction result = session.getTransaction();

		if (result == null || !result.isActive()) {
			result = session.beginTransaction();
		}

		return result;
	}
	
	public static Session getSession() {
		Session result = SESSION_FACTORY.getCurrentSession();
		result.setFlushMode(FlushMode.MANUAL);
		
		if (!result.getTransaction().isActive()) {
			result.beginTransaction();
		}
		
		return result;
	}
	
	public static void commit() {
		commit(true);
	}
	
	public static void commit(boolean startNewTransaction) {
		closeSession();

		if (startNewTransaction) {
			SESSION_FACTORY.openSession().beginTransaction();
		}
	}
	
	public static void closeSession() {
		Session session = getSession();
		
		if (session.getTransaction().isActive()) {
			session.flush();
			session.getTransaction().commit();
		}
	}

	public static ResultSet executeSelectQuery(final String query) {
		Session session = HibernateUtils.getSession();
		
		// This has to be final to be reachable from the Work implementation
		final ResultSet[] result = new ResultSet[1];
		
		session.doWork(new Work() {
			public void execute(Connection connection) throws SQLException {
				Statement statement = connection.createStatement();
				result[0] = statement.executeQuery(query);
			}
		});
		
		return result[0];
	}

	public static ModelObject getElementById(Class<? extends ModelObject> modelObjectClass, String id) {
		ModelObject modelObjectTemplate = (ModelObject)new BeanWrapperImpl(modelObjectClass).getWrappedInstance();
		BaseModelObjectDAO<ModelObject> modelObjectDAO = new BaseModelObjectDAO<ModelObject>(modelObjectClass);
		
		modelObjectTemplate.setId(id);
		
		return modelObjectDAO.findByExample(modelObjectTemplate).get(0);
	}

	public static Connection getJDBCConnection() {
        try {
			Class.forName(CONFIGURATION.getProperty("hibernate.connection.driver_class"));
		} catch (ClassNotFoundException e) {
			throw new NeoGeoException("Unable to load database driver", e);
		}
        
        String connectionString = CONFIGURATION.getProperty("hibernate.connection.url");
        String username = CONFIGURATION.getProperty("hibernate.connection.username");
        String password = CONFIGURATION.getProperty("hibernate.connection.password");
        
		Connection connection;
		
		try {
			connection = DriverManager.getConnection(connectionString, username, password);
		} catch (SQLException e) {
			throw new NeoGeoException("Unable to create database connection", e);
		}

		return connection;
	}
}
