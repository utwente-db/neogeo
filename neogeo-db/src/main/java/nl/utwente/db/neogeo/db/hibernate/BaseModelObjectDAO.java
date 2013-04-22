package nl.utwente.db.neogeo.db.hibernate;

import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import nl.utwente.db.neogeo.core.model.ModelObject;
import nl.utwente.db.neogeo.db.utils.HibernateUtils;
import nl.utwente.db.neogeo.utils.StringUtils;

import org.apache.log4j.Logger;
import org.hibernate.Criteria;
import org.hibernate.criterion.Criterion;
import org.hibernate.criterion.Example;
import org.springframework.beans.BeanWrapperImpl;

public class BaseModelObjectDAO<Type extends ModelObject> implements GenericDAO<Type> {

	private Class<? extends Type> persistentClass;
	private Logger logger = Logger.getLogger(BaseModelObjectDAO.class);
	
	@SuppressWarnings("unchecked")
	protected BaseModelObjectDAO() {
		this.persistentClass = (Class<Type>) ((ParameterizedType) getClass()
				.getGenericSuperclass()).getActualTypeArguments()[0];
	}
	
	public BaseModelObjectDAO(Class<? extends Type> clazz) {
		this.persistentClass = clazz;
	}
	
	public Class<? extends Type> getPersistentClass() {
		return persistentClass;
	}

	@SuppressWarnings("unchecked")
	public List<Type> findAll() {
		return findByExample((Type)new BeanWrapperImpl(getPersistentClass()).getWrappedInstance());
	}

	@SuppressWarnings("unchecked")
	public Type getById(String id) {
		return (Type)HibernateUtils.getSession().load(getPersistentClass(), id);
	}

	/**
	 * Override for types with primitive fields!
	 * @param exampleInstance
	 * @return
	 */
	public List<Type> findByExample(Type exampleInstance) {
		return findByExample(exampleInstance, new ArrayList<String>());
	}
	
	@SuppressWarnings("unchecked")
	public List<Type> findByExample(Type exampleInstance, Iterable<String> excludeProperties) {
		if (!HibernateUtils.getSession().getTransaction().isActive()) {
			HibernateUtils.getSession().beginTransaction();
		}
		
		Criteria crit = HibernateUtils.getSession().createCriteria(getPersistentClass());
		Example example =  Example.create(exampleInstance);

		for (String exclude : excludeProperties) {
			example.excludeProperty(exclude);
		}
		
		crit.add(example);
		
		return crit.list();
	}

	public Type makePersistent(Type entity) {
		if (entity instanceof ModelObject) {
			ModelObject modelObject = entity;
			
			if (StringUtils.isEmpty(modelObject.getId())) {
				modelObject.setId(UUID.randomUUID().toString());
			}
		}
		
		HibernateUtils.getSession().saveOrUpdate(entity);

		return entity;
	}

	public void insert(Type entity) {
		logger.info("Storing " + entity);
		HibernateUtils.getSession().save(entity);
	}

	public void makeTransient(Type entity) {
		HibernateUtils.getSession().delete(entity);
	}

	public void flush() {
		HibernateUtils.getSession().flush();
	}

	public void clear() {
		HibernateUtils.getSession().clear();
	}

	/**
	 * Use this inside subclasses as a convenience method.
	 */
	@SuppressWarnings("unchecked")
	protected List<Type> findByCriteria(Criterion... criterion) {
		Criteria crit = HibernateUtils.getSession().createCriteria(getPersistentClass());

		for (Criterion c : criterion) {
			crit.add(c);
		}

		return crit.list();
	}

}