/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nl.pallett.median.generate.pegel;

import java.sql.SQLException;
import java.util.HashMap;
import nl.pallett.median.generate.ProcessPart;

/**
 *
 * @author Dennis
 */
public abstract class ProcessPegelPart extends ProcessPart {
    protected String sortedTable;
    
    protected String dataTable;
    
    protected String shiftTable;
    
    protected String medianTable;

    protected long medianTime1 = -1;
    
    protected long medianTime2 = -1;
        
    protected int medianValue1;
    
    protected int medianValue2;
    
    protected HashMap<Long, Integer> leftShifts;
    
    protected HashMap<Long, Integer> rightShifts;    
    
    public void setTables (String dataTable, String shiftTable, String medianTable) {
        this.dataTable = dataTable;
        this.shiftTable = shiftTable;
        this.medianTable = medianTable;
    }
          
    protected void generateShifts () throws SQLException {
        // calculate left shifts
        calculateLeftShifts ();

        // calculate right shifts
        calculateRightShifts ();

        // insert shift data
        insertShifts();
    }
        
    protected abstract void calculateLeftShifts () throws SQLException;
    protected abstract void calculateRightShifts () throws SQLException;
    protected abstract void insertShifts () throws SQLException;    
}
