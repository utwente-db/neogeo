package nl.utwente.db.neogeo.db.hibernate;

import java.util.List;

public interface GenericDAO<Type extends Object> {

	public List<Type> findAll();
	public List<Type> findByExample(Type exampleInstance, Iterable<String> excludeProperty);

	public Type makePersistent(Type entity);
	public void insert(Type entity);

	public void makeTransient(Type entity);
}