/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nl.pallett.median.generate.knmi;

import java.io.IOException;
import java.sql.SQLException;
import nl.pallett.median.generate.ProcessPart;
import org.apache.log4j.Logger;

/**
 *
 * @author Dennis
 */
public abstract class ProcessKnmiPart extends ProcessPart {
    private static final Logger log = Logger.getLogger(ProcessKnmiPart.class);
    
    public static final int STATION_START = 1;
    
    public static final int STATION_END = 10;
    
    public static final int STATION_STEP = 1;
        
    protected String dataTable;
    
    protected String timeShiftTable;
    
    protected String medianTable;
    
    protected String stationShiftTable;
    
    protected int medianRowId1 = -1;
    
    protected int medianTime1 = -1;
    
    protected int medianStation1 = -1;
    
    protected int medianRowId2 = -1;
    
    protected int medianTime2 = -1;
    
    protected int medianStation2 = -1;
    
    protected short medianTemperature1 = -1;
    
    protected short medianTemperature2 = -1;
    
    public void setTables (String dataTable, String stationShiftTable, String timeShiftTable, String medianTable) {
        this.dataTable = dataTable;
        this.timeShiftTable = timeShiftTable;
        this.medianTable = medianTable;
        this.stationShiftTable = stationShiftTable;
    }
        
    protected void generateShifts () throws SQLException {
        calculateLeftShiftsForStation ();

        calculateRightShiftsForStation ();

        insertShiftsForStation();

        calculateLeftShiftsForTime ();

        calculateRightShiftsForTime();

        insertShiftsForTime();
    }
    
    protected abstract void calculateLeftShiftsForStation () throws SQLException;
    protected abstract void calculateRightShiftsForStation () throws SQLException;
    protected abstract void insertShiftsForStation () throws SQLException;
    protected abstract void calculateLeftShiftsForTime () throws SQLException;
    protected abstract void calculateRightShiftsForTime () throws SQLException;
    protected abstract void insertShiftsForTime () throws SQLException;
    
}
