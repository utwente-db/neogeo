package nl.utwente.db.neogeo.preaggregate.mapreduce;

/**
 *
 * @author Dennis Pallett <dennis@pallett.nl>
 */
public enum AGGR_TYPE {
    TYPE_DOUBLE ("double"), 
    TYPE_INT ("int"), 
    TYPE_BIGINT ("bigint");
    
    private String sqlName;
    
    private AGGR_TYPE (String sqlName) {
        this.sqlName = sqlName;
    }
    
    public String sqlName () {
        return this.sqlName;
    }
}
