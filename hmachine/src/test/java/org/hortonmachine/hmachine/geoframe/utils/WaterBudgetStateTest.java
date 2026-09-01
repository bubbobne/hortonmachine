package org.hortonmachine.hmachine.geoframe.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.hortonmachine.dbs.compat.ASpatialDb;
import org.hortonmachine.dbs.compat.EDb;
import org.hortonmachine.gears.libs.modules.HMConstants;
import org.hortonmachine.hmachine.geoframe.io.database.TableUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Covers the warm-start plumbing: a fixed (non-timestamped) state table that
 * can be appended to across runs, and reading back the last state per basin
 * as {@link WaterBudgetInitialConditions} to resume a simulation.
 */
public class WaterBudgetStateTest {

    private File tmpGpkg;
    private ASpatialDb db;
    private static final String TABLE_NAME = "test_water_budget_state";

    @Before
    public void openTempDb() throws Exception {
        tmpGpkg = File.createTempFile("hm_test_water_budget_state_", "." + HMConstants.GPKG);
        tmpGpkg.delete();
        db = EDb.GEOPACKAGE.getSpatialDb();
        db.open(tmpGpkg.getAbsolutePath());
    }

    @After
    public void closeTempDb() throws Exception {
        db.close();
        tmpGpkg.delete();
    }

    @Test
    public void getLastSimulationStep_returnsMinusOne_whenTableDoesNotExistYet() throws Exception {
        long lastStep = TableUtils.getLastSimulationStep(db, TABLE_NAME);

        assertEquals(-1, lastStep);
    }

    @Test
    public void loadLastState_returnsDefaults_whenTableDoesNotExistYet() throws Exception {
        WaterBudgetInitialConditions conditions = WaterBudgetState.loadLastState(db, TABLE_NAME, 2);

        assertEquals(0.0, conditions.solidWater[1], 0.0);
        assertTrue(Double.isNaN(conditions.canopy[1]));
    }

    @Test
    public void getLastSimulationStep_andLoadLastState_reflectTheMostRecentTimestepAcrossBasins() throws Exception {
        WaterBudgetState.initTable(db, TABLE_NAME);
        insertState(1, 1000L, 1.0, 2.0);
        insertState(2, 1000L, 3.0, 4.0);
        insertState(1, 2000L, 10.0, 20.0);
        insertState(2, 2000L, 30.0, 40.0);

        long lastStep = TableUtils.getLastSimulationStep(db, TABLE_NAME);
        assertEquals(2000L, lastStep);

        WaterBudgetInitialConditions conditions = WaterBudgetState.loadLastState(db, TABLE_NAME, 2);
        assertEquals(10.0, conditions.solidWater[1], 0.0);
        assertEquals(20.0, conditions.liquidWater[1], 0.0);
        assertEquals(30.0, conditions.solidWater[2], 0.0);
        assertEquals(40.0, conditions.liquidWater[2], 0.0);
    }

    private void insertState(int basinId, long timestamp, double solidWaterFinal, double liquidWaterFinal)
            throws Exception {
        WaterBudgetState state = new WaterBudgetState();
        state.solidWaterFinal = solidWaterFinal;
        state.liquidWaterFinal = liquidWaterFinal;
        state.canopyFinal = Double.NaN;
        state.rootzoneFinal = Double.NaN;
        state.runoffFinal = Double.NaN;
        state.groundFinal = Double.NaN;

        String sql = WaterBudgetState.getPreparedInsertSql(TABLE_NAME);
        List<Object[]> rows = new ArrayList<>();
        rows.add(state.getInsertIntoDbObjects(basinId, timestamp));
        db.executeBatchPreparedSql(sql, rows);
    }
}
