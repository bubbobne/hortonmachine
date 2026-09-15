package org.hortonmachine.hmachine.geoframe.io.database;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import org.hortonmachine.dbs.compat.ASpatialDb;
import org.hortonmachine.dbs.compat.objects.QueryResult;
import org.hortonmachine.gears.libs.modules.HMConstants;
import org.hortonmachine.hmachine.geoframe.io.database.tables.GeoFrameSimpleTable;
import org.hortonmachine.hmachine.geoframe.io.database.tables.definition.TableField;
import org.hortonmachine.hmachine.geoframe.io.database.tables.implementation.BasinDataSchema.BasinDataField;
import org.hortonmachine.hmachine.geoframe.io.database.tables.implementation.SimulationSchema.SimulationField;
import org.hortonmachine.hmachine.geoframe.io.database.tables.implementation.StationDataSchema;
import org.hortonmachine.hmachine.geoframe.io.database.tables.implementation.VarSchema.EnvironmentalVariable;
import org.hortonmachine.hmachine.geoframe.io.database.tables.implementation.VarSchema.EnvironmentalVariableType;
import org.hortonmachine.hmachine.geoframe.io.database.tables.implementation.VarSchema.TimeResolution;
import org.hortonmachine.hmachine.geoframe.io.database.tables.implementation.VarSchema.VarField;

/**
 * Utility class for creating and validating database tables and populating
 * standard reference data such as variable definitions ({@link VarField}).
 *
 * @author Daniele Andreis
 */
public class TableUtils {
	/**
	 * Get the list of the variable.
	 * 
	 * @param resolution, the time resolution, can be hourly/daily.
	 * @return
	 */
	public final static List<EnvironmentalVariable> getFixedEnviramentalVariable(TimeResolution resolution) {

		String mmFlux = "mm/";
		String temperatureUnit = "°C";

		// Radiation is always a flux and in W/m² and is always related to the size of
		// the timestep.
		String radiationUnit = "W/m²";
		String dischargeUnit = "m³/s";

		if (resolution != null) {
			switch (resolution) {
			case HOURLY -> mmFlux = mmFlux + "h";
			case DAILY -> mmFlux = mmFlux + "day";
			case MONTHLY -> mmFlux = mmFlux + "month";
			case YEARLY -> mmFlux = mmFlux + "year";
			}
		} else {
			mmFlux = null;
		}

		/**
		 * TODO we can define potential evapotranspiration and actual
		 * evapotranspiration????
		 */
		return List.of(
				new EnvironmentalVariable(EnvironmentalVariableType.EVAPOTRANSPIRATION.getId(), "Evapotranspiration",
						mmFlux, "Evapotraspiration"),
				new EnvironmentalVariable(EnvironmentalVariableType.PRECIPITATION.getId(), "Precipitation", mmFlux,
						"Accumulated precipitation"),
				new EnvironmentalVariable(EnvironmentalVariableType.TEMPERATURE.getId(), "Temperature", temperatureUnit,
						"Air temperature"),
				new EnvironmentalVariable(EnvironmentalVariableType.RADIATION.getId(), "Radiation", radiationUnit,
						"Incoming solar radiation"),
				new EnvironmentalVariable(EnvironmentalVariableType.DISCHARGE.getId(), "Discharge", dischargeUnit,
						"River discharge"));
	}

	/**
	 * Recreates the HashMap structure required by legacy models.
	 *
	 * Most models exchange data through a HashMap where the key represents an ID,
	 * such as a basin ID or point ID, and the associated value is a double[] array,
	 * often containing only one element. This method recreates this structure from
	 * the provided values and IDs to ensure compatibility with legacy models.
	 *
	 * @param values the values to store in the HashMap
	 * @param ids    the IDs associated with the values
	 * @return a HashMap compatible with legacy models
	 */
	public final static HashMap<Integer, double[]> getLegacyHMInput(double[] h, int[] ids) {
		HashMap<Integer, double[]> data = new HashMap<Integer, double[]>();
		try {
			for (int i = 0; i < ids.length; i++) {
				data.put(ids[i], new double[] { h[ids[i]] });
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return data;
	}

	public final static int[] getIntIdArray(ASpatialDb inGeoframeDb, String tableName, String columnName,
			String where) {
		QueryResult result;
		try {
			String sql = "select * from " + tableName;
			if (where != null) {
				sql = sql + " " + where;
			}
			result = inGeoframeDb.getTableRecordsMapFromRawSql(sql, -1);

			int idIndex = result.names.indexOf(columnName);

			var rows = result.data;
			int l = result.data.size();
			int[] ids = new int[l];
			for (int i = 0; i < l; i++) {
				ids[i] = ((Number) rows.get(i)[idIndex]).intValue();
			}
			return ids;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;

	}

	/**
	 * Recreates the HashMap structure required by legacy models filled with NaN
	 * value.
	 *
	 * @param ids the IDs associated with the values
	 * @return a HashMap compatible with legacy models with NaN.
	 */
	public final static HashMap<Integer, double[]> getLegacyHMInputNaN(int[] ids) {
		// TODO Auto-generated method stub
		HashMap<Integer, double[]> data = new HashMap<Integer, double[]>();
		for (int i = 0; i < ids.length; i++) {
			data.put(ids[i], new double[] { HMConstants.doubleNovalue });
		}
		return data;
	}

	/**
	 * Checks whether the station input data are sufficiently time-aligned for the
	 * required meteorological variables.
	 * <p>
	 * For both precipitation and temperature, the method determines how many of the
	 * requested stations share the same latest timestamp. The data are considered
	 * time-aligned when the number of aligned stations is greater than or equal to
	 * the specified threshold for both variables.
	 *
	 * @param db        the database containing the station data
	 * @param ids       the IDs of the stations to check
	 * @param threshold the minimum number of stations that must share the same
	 *                  latest timestamp
	 * @return {@code true} if both precipitation and temperature satisfy the
	 *         alignment threshold, {@code false} otherwise
	 */

	public final static boolean areDBStationInputDataTimeAligned(ASpatialDb db, int[] ids, int threshold) {

		int precipitationStatus = numberOfTableValueTimeAligned(db, ids, GeoFrameSimpleTable.STATIONDATA.name(),
				StationDataSchema.StationDataField.STATION_ID.columnName(),
				StationDataSchema.StationDataField.TS.columnName(),
				StationDataSchema.StationDataField.VAR_ID.columnName(),
				EnvironmentalVariableType.PRECIPITATION.getId());

		int temperatureStatus = numberOfTableValueTimeAligned(db, ids, GeoFrameSimpleTable.STATIONDATA.name(),
				StationDataSchema.StationDataField.STATION_ID.columnName(),
				StationDataSchema.StationDataField.TS.columnName(),
				StationDataSchema.StationDataField.VAR_ID.columnName(), EnvironmentalVariableType.TEMPERATURE.getId());

		return temperatureStatus >= threshold && precipitationStatus >= threshold;

	}

	/**
	 * Checks whether the input  basin data are time-aligned.
	 * <p>
	 * The latest available timestamp is checked for all the requested basin IDs and
	 * for each required simulated variable: precipitation, temperature, and
	 * evapotranspiration. The data are considered time-aligned only if all basin
	 * IDs share the same latest timestamp for each variable.
	 *
	 * @param db  the database containing the simulated basin data
	 * @param ids the IDs of the basins to check
	 * @return {@code true} if all requested basins are time-aligned for all
	 *         required variables, {@code false} otherwise
	 */
	public final static boolean areDBStationInputDataTimeAligned(ASpatialDb db, int[] ids) {

		boolean precipitationStatus = areTableValueTimeAligned(db, ids, GeoFrameSimpleTable.BASINDATA.name(),
				BasinDataField.BASIN_ID.columnName(), BasinDataField.TS.columnName(),
				BasinDataField.VAR_ID.columnName(), EnvironmentalVariableType.PRECIPITATION.getId());

		boolean temperatureStatus = areTableValueTimeAligned(db, ids, GeoFrameSimpleTable.BASINDATA.name(),
				BasinDataField.BASIN_ID.columnName(), BasinDataField.TS.columnName(),
				BasinDataField.VAR_ID.columnName(), EnvironmentalVariableType.TEMPERATURE.getId());

		boolean etStatus = areTableValueTimeAligned(db, ids, GeoFrameSimpleTable.BASINDATA.name(),
				BasinDataField.BASIN_ID.columnName(), BasinDataField.TS.columnName(),
				BasinDataField.VAR_ID.columnName(), EnvironmentalVariableType.EVAPOTRANSPIRATION.getId());

		return precipitationStatus && temperatureStatus && etStatus;
	}

	/**
	 * Returns the last simulated timestep found in a water budget state table (see
	 * {@code WaterBudgetState}), i.e. the point a realtime/daily job should resume
	 * simulating from.
	 * <p>
	 * State rows for every basin are always written together in a single batch per
	 * timestep (see {@code WaterBudgetSimulation.processTimestep}), so the
	 * table-wide {@code MAX(timestamp)} is always aligned across basins - no
	 * per-basin grouping is needed.
	 *
	 * @param db             the database containing the state table
	 * @param stateTableName the state table to check (see
	 *                       {@code WaterBudgetState#initTable})
	 * @return the last simulated timestamp, or {@code -1} if the table does not
	 *         exist yet (no simulation has ever been run)
	 */

	public final static long getLastStepTimestamp(ASpatialDb db, String stateTableName) throws Exception {
		return getLastStepTimestamp(db, stateTableName, SimulationField.TS);
	}

	/**
	 * Returns the last  timestep found in a table 

	 *
	 * @param db             the database containing the state table
	 * @param stateTableName the state table to check 
	 * @param tableField the field to check.
	 * @return the last simulated timestamp, or {@code -1} if the table does not
	 *         exist yet (no simulation has ever been run)
	 */
	public final static long getLastStepTimestamp(ASpatialDb db, String stateTableName, TableField tableField)
			throws Exception {
		if (!db.hasTable(stateTableName)) {
			return -1;
		}
		Long maxTs = db.getLong("SELECT MAX(" + tableField.columnName() + ") FROM " + stateTableName);
		return maxTs == null ? -1 : maxTs;
	}

	/**
	 * Checks whether all the requested IDs are time-aligned for a specific
	 * variable.
	 * <p>
	 * Two IDs are considered time-aligned when their latest available records,
	 * identified by the maximum timestamp, have the same timestamp. The method
	 * returns {@code true} only if all requested IDs share the same latest
	 * timestamp.
	 *
	 * @param db             the database containing the data
	 * @param ids            the IDs to check
	 * @param tableName      the name of the table containing the data
	 * @param idColumn       the name of the column containing the IDs
	 * @param tsColumn       the name of the timestamp column
	 * @param variableColumn the name of the column identifying the variable
	 * @param variable       the ID of the variable to check
	 * @return {@code true} if all requested IDs share the same latest timestamp,
	 *         {@code false} otherwise
	 */
	public final static boolean areTableValueTimeAligned(ASpatialDb db, int[] ids, String tableName, String idColumn,
			String tsColumn, String variableColumn, int variable) {
		int n = TableUtils.numberOfTableValueTimeAligned(db, ids, tableName, idColumn, tsColumn, variableColumn,
				variable);
		if (n != ids.length) {
			return false;
		}
		return true;
	}

	/**
	 * Returns the number of IDs whose latest available timestamp is aligned with
	 * the most recent timestamp encountered for the specified variable.
	 * <p>
	 * For each requested ID, the latest available timestamp is retrieved using
	 * {@code MAX(ts)}. While iterating over the results, the most recent timestamp
	 * encountered is used as the reference timestamp. The counter is reset whenever
	 * a newer timestamp is found and incremented for each subsequent ID sharing the
	 * same timestamp.
	 * <p>
	 * If one or more requested IDs have no data for the specified variable, the
	 * method returns {@code 0}.
	 *
	 * @param db             the database containing the data
	 * @param ids            the IDs to check
	 * @param tableName      the name of the table containing the data
	 * @param idColumn       the name of the column containing the IDs
	 * @param tsColumn       the name of the timestamp column
	 * @param variableColumn the name of the column identifying the variable
	 * @param variable       the ID of the variable to check
	 * @return the number of IDs aligned with the most recent timestamp encountered,
	 *         or {@code 0} if the requested data are incomplete or unavailable
	 */
	public final static int numberOfTableValueTimeAligned(ASpatialDb db, int[] ids, String tableName, String idColumn,
			String tsColumn, String variableColumn, int variable) {
		if (ids == null || ids.length == 0) {
			return 0;
		}
		int n = 0;
		try {
			String idList = Arrays.stream(ids).mapToObj(String::valueOf).collect(Collectors.joining(","));
			StringBuilder sql = new StringBuilder();
			sql.append("SELECT ").append(idColumn).append(", MAX(").append(tsColumn).append(") AS max_ts ")
					.append("FROM ").append(tableName).append(" WHERE ").append(variableColumn).append("=")
					.append(variable).append(" and ").append(idColumn).append(" IN (").append(idList).append(")");

			sql.append(" GROUP BY ").append(idColumn);

			QueryResult result = db.getTableRecordsMapFromRawSql(sql.toString(), -1);
			// Some requested IDs are missing
			if (result.data.size() != ids.length) {
				return 0;
			}
			int tsIndex = result.names.indexOf("max_ts");
			Long expectedTs = null;
			for (Object[] row : result.data) {
				long ts = ((Number) row[tsIndex]).longValue();
				if (expectedTs == null || ts > expectedTs) {
					expectedTs = ts;
					n = 1;
				} else if (expectedTs.longValue() == ts) {
					n = n + 1;
				}
			}
			return n;
		} catch (Exception e) {
			e.printStackTrace();
			return 0;
		}
	}

}
