package nl.utwente.db.neogeo.core.model;

import java.util.Date;

public abstract class BaseModelObject implements ModelObject {
	protected String id;
	protected Date timestamp;
	protected String name;

	public BaseModelObject() {
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public Date getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Date timestamp) {
		this.timestamp = timestamp;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	@Override
	public boolean equals(Object other) {
		if (other.getClass().isInstance(this)) {
			if (((BaseModelObject)other).getId().equals(this.getId())) {
				return true;
			}
		}

		return false;
	}

	@Override
	public int hashCode() {
		return (this.getClass().getSimpleName() + ":" + this.id).hashCode();
	}


	@Override
	public String toString() {
		return this.getClass().getSimpleName() + " [id=" + id + ", timestamp=" + timestamp
				+ ", name=" + name + "]";
	}
}