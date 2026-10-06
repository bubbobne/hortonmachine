package org.hortonmachine.hmachine.geoframe.io.database;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.hortonmachine.dbs.compat.ASpatialDb;
import org.hortonmachine.dbs.compat.objects.QueryResult;
import org.hortonmachine.gears.libs.modules.HMConstants;
import org.hortonmachine.hmachine.geoframe.io.database.tables.GeoFrameGeoTable;
import org.hortonmachine.hmachine.geoframe.io.database.tables.GeoFrameSimpleTable;
import org.hortonmachine.hmachine.geoframe.io.database.tables.definition.TableField;
import org.hortonmachine.hmachine.geoframe.io.database.tables.implementation.BasinDataSchema.BasinDataField;
import org.hortonmachine.hmachine.geoframe.io.database.tables.implementation.BasinPolygonSchema.BasinMultiPolygonField;
import org.hortonmachine.hmachine.geoframe.io.database.tables.implementation.SimulationSchema.SimulationField;
import org.hortonmachine.hmachine.geoframe.io.database.tables.implementation.StationDataSchema;
import org.hortonmachine.hmachine.geoframe.io.database.tables.implementation.StationDataSchema.StationDataField;
import org.hortonmachine.hmachine.geoframe.io.database.tables.implementation.TopologySchema.TopologyField;
import org.hortonmachine.hmachine.geoframe.io.database.tables.implementation.VariableSchema.EnvironmentalVariable;
import org.hortonmachine.hmachine.geoframe.io.database.tables.implementation.VariableSchema.EnvironmentalVariableType;
import org.hortonmachine.hmachine.geoframe.io.database.tables.implementation.VariableSchema.TimeResolution;
import org.hortonmachine.hmachine.geoframe.io.database.tables.implementation.VariableSchema.VarField;
import org.hortonmachine.hmachine.geoframe.utils.TopologyUtilities;
import org.hortonmachine.hmachine.geoframe.utils.WaterBudgetState;

/**
 * Utility class for creating and validating database tables and populating
 * standard reference data such as variable definitions ({@link VarField}).
 *
 * @author Daniele Andreis
 */
public class TableUtils {

	private static final String IS_VALID_VALUE_SQL = "value IS NOT NULL AND value <> " + HMConstants.doubleNovalue;
	private static final String BASIN_ID = BasinDataField.BASIN_ID.columnName();
	private static final String TS = BasinDataField.TS.columnName();
	private static final String VAR_ID = BasinDataField.VAR_ID.columnName();

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
	 * Number of distinct basins in the {@code basin} table, or 0 if the table is
	 * missing or empty.
	 */
	public final static long countBasins(ASpatialDb db) throws Exception {
		return TableUtils.countRecord(db, GeoFrameGeoTable.BASIN.tableName(), BasinMultiPolygonField.ID.columnName());
	}

	/**
	 * Number of distinct basins in the {@code topology} table, or 0 if the table is
	 * missing or empty.
	 * <p>
	 * This is the actual numeber of basin involved in theERM model.
	 * </p>
	 * 
	 */
	public final static long countActualBasins(ASpatialDb db) throws Exception {
		return TableUtils.countRecord(db, GeoFrameSimpleTable.TOPOLOGY.tableName(),
				TopologyField.UPPSTREAM_BASIN.columnName());
	}

	/**
	 * Number of distinct record in a table providing a column
	 */
	public final static long countRecord(ASpatialDb db, String tableName, String columnName) throws Exception {
		if (!db.hasTable(tableName)) {
			return 0;
		}
		Long count = db.getLong("SELECT COUNT(DISTINCT " + columnName + ") FROM " + tableName);
		return count == null ? 0 : count;
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
	 * 
	 * Checks whether the simulated basin data are time-aligned. realtime-simulation
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
	public final static boolean areDBSimulatedDataTimeAligned(ASpatialDb db, int[] ids) {

		boolean precipitationStatus = areTableValueTimeAligned(db, ids, GeoFrameSimpleTable.BASINDATA.tableName(),
				BasinDataField.BASIN_ID.columnName(), BasinDataField.TS.columnName(),
				BasinDataField.VAR_ID.columnName(), EnvironmentalVariableType.PRECIPITATION.getId());

		boolean temperatureStatus = areTableValueTimeAligned(db, ids, GeoFrameSimpleTable.BASINDATA.tableName(),
				BasinDataField.BASIN_ID.columnName(), BasinDataField.TS.columnName(),
				BasinDataField.VAR_ID.columnName(), EnvironmentalVariableType.TEMPERATURE.getId());

		boolean etStatus = areTableValueTimeAligned(db, ids, GeoFrameSimpleTable.BASINDATA.tableName(),
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
	 * realtime-simulation table-wide {@code MAX(timestamp)} is always aligned
	 * across basins - no per-basin grouping is needed.
	 *
	 * @param db             the database containing the state table
	 * @param stateTableName the state table to check (see
	 *                       {@code WaterBudgetState#initTable})
	 * @return the last simulated timestamp, or {@code -1} if the table does not
	 *         exist yet (no simulation has ever been run)
	 */

	public final static long getLastSimulatedTimestamp(ASpatialDb db, String simulationDischargeTable) throws Exception {
		String columnName = SimulationField.TS.columnName();
		int basinId = TopologyUtilities.getRootNodeFromDb(db).basinId;
		if (basinId > 0) {
			return getLastStepTimestamp(db, simulationDischargeTable, columnName,
					SimulationField.BASIN_ID.columnName() + "=" + basinId);
		}
		return getLastStepTimestamp(db, simulationDischargeTable, columnName, null);
	}

	/**
	 * Returns the last timestep found in a table
	 *
	 * 
	 * @param db             the database containing the state table
	 * @param stateTableName the state table to check
	 * @param tableField     the field to check.
	 * @return the last simulated timestamp, or {@code -1} if the table does not
	 *         exist yet (no simulation has ever been run)
	 */
	public final static long getLastStepTimestamp(ASpatialDb db, String stateTableName, String tableField, String where)
			throws Exception {
		if (!db.hasTable(stateTableName)) {
			return -1;
		}
		String query = "SELECT MAX(" + tableField + ") FROM " + stateTableName;
		if (where != null) {
			query = query + " where " + where;
		}
		Long maxTs = db.getLong(query);
		return maxTs == null ? -1 : maxTs;
	}

	/**
	 * The last hour for which every basin of the topology has a valid value for all
	 * the simulation inputs, or {@code -1} if there is none yet.
	 */
	public final static long lastSimulableTimestamp(ASpatialDb db, List<Integer> topologyBasinIds,
			int[] simulationInput) throws Exception {
		long lastTs = Long.MAX_VALUE;
		for (int varId : simulationInput) {
			lastTs = Math.min(lastTs, TableUtils.lastFullyCoveredBasinTimestamp(db, varId, topologyBasinIds));
		}
		return lastTs;
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

	private record BasinSelection(Set<Integer> basinIds, long basinCount) {
	}

	private static String basinFilterSql(BasinSelection basinSelection) {
		return basinFilterSql(basinSelection.basinIds);
	}

	/**
	 * Creates the SQL condition used to restrict {@code BASINDATA} records to
	 * either the specified basin identifiers or all basins listed in the
	 * {@code basin} table.
	 *
	 * @param basinIds the selected basin identifiers, or {@code null} to select all
	 *                 known basins
	 * @return the SQL condition used to filter basin identifiers
	 */
	private static String basinFilterSql(Set<Integer> basinIds) {
		// the unary + keeps SQLite from answering the IN with idx_basin_data_basin_ts:
		// that index has neither var_id nor value, so every row costs a random table
		// lookup (~7x slower than the ts range scan on the primary key)
		if (basinIds != null && !basinIds.isEmpty()) {
			String basinIdsSql = basinIds.stream().map(String::valueOf).collect(Collectors.joining(","));

			return "+" + BASIN_ID + " IN (" + basinIdsSql + ")";
		}

		return "+" + BASIN_ID + " IN (SELECT " + BasinMultiPolygonField.ID.columnName() + " FROM "
				+ GeoFrameGeoTable.BASIN.tableName() + ")";
	}

	private static BasinSelection resolveBasinSelection(ASpatialDb db, Collection<Integer> basinIds) throws Exception {

		if (basinIds == null) {
			return new BasinSelection(null, countBasins(db));
		}

		Set<Integer> uniqueBasinIds = basinIds.stream().filter(Objects::nonNull)
				.collect(Collectors.toCollection(LinkedHashSet::new));

		return new BasinSelection(uniqueBasinIds, uniqueBasinIds.size());
	}

	/**
	 * Default for
	 * {@link #lastFullyCoveredBasinTimestamp(ASpatialDb, int, Collection)}: how far
	 * back from the most recent row the search for a fully covered timestamp is
	 * allowed to go.
	 */
	public static final long DEFAULT_MAX_LOOKBACK_MILLIS = 30L * 24 * 3600 * 1000;

	/**
	 * Same as
	 * {@link #lastFullyCoveredBasinTimestamp(ASpatialDb, int, Collection, long)}
	 * with {@link #DEFAULT_MAX_LOOKBACK_MILLIS}.
	 */
	public static long lastFullyCoveredBasinTimestamp(ASpatialDb db, int varId, Collection<Integer> basinIds)
			throws Exception {
		return lastFullyCoveredBasinTimestamp(db, varId, basinIds, DEFAULT_MAX_LOOKBACK_MILLIS);
	}

	/**
	 * Returns the latest timestamp for which every selected basin has a valid value
	 * for the specified variable in {@code BASINDATA}.
	 * <p>
	 * If {@code basinIds} is {@code null}, all basins listed in the {@code basin}
	 * table are checked. Otherwise, the check is limited to the provided basin
	 * identifiers.
	 * </p>
	 * <p>
	 * The search only looks at the {@code maxLookbackMillis} before the most recent
	 * row for {@code varId}: a coverage gap that old is not a leftover of an
	 * interrupted run but a real problem (e.g. a basin that is never computed), so
	 * an exception is thrown instead of silently restarting from scratch.
	 * </p>
	 *
	 * @param db                the database containing the basin data
	 * @param varId             the variable identifier
	 * @param basinIds          the basin identifiers to check, or {@code null} to
	 *                          check all known basins
	 * @param maxLookbackMillis how far back from the most recent row to search
	 * @return the latest fully covered timestamp, or {@code -1} if there is none
	 *         because there is no data (or only partially covered data, all within
	 *         the lookback window)
	 * @throws IllegalStateException if data older than the lookback window exists
	 *                               but no timestamp inside the window is fully
	 *                               covered
	 * @throws Exception             if an error occurs while querying the database
	 */
	public static long lastFullyCoveredBasinTimestamp(ASpatialDb db, int varId, Collection<Integer> basinIds,
			long maxLookbackMillis) throws Exception {

		String tableName = GeoFrameSimpleTable.BASINDATA.tableName();
		if (!db.hasTable(tableName) || !db.hasTable(GeoFrameGeoTable.BASIN.tableName())) {
			return -1;
		}
		var selection = resolveBasinSelection(db, basinIds);

		if (selection.basinCount == 0) {
			return -1;
		}

		// no basin filter here: it would prevent SQLite from using the primary key
		// index and the window below includes every basin anyway
		// getLong returns 0 for a SQL NULL, hence the COALESCE
		Long maxTs = db
				.getLong("SELECT COALESCE(MAX(" + TS + "), -1) FROM " + tableName + " WHERE " + VAR_ID + " = " + varId);
		if (maxTs == null || maxTs < 0) {
			return -1;
		}
		long windowStart = maxTs - maxLookbackMillis;

		Long lastCoveredTs = db.getLong("SELECT COALESCE(MAX(" + TS + "), -1) FROM (SELECT " + TS + " FROM " + tableName
				+ " WHERE " + VAR_ID + " = " + varId + " AND " + TS + " > " + windowStart + " AND " + TS + " <= "
				+ maxTs + " AND " + IS_VALID_VALUE_SQL + " AND " + basinFilterSql(selection.basinIds) + " GROUP BY "
				+ TS + " HAVING COUNT(DISTINCT " + BASIN_ID + ") >= " + selection.basinCount + ")");
		if (lastCoveredTs != null && lastCoveredTs >= 0) {
			return lastCoveredTs;
		}

		Long olderRows = db.getLong("SELECT COUNT(*) FROM (SELECT 1 FROM " + tableName + " WHERE " + VAR_ID + " = "
				+ varId + " AND " + TS + " <= " + windowStart + " LIMIT 1)");
		if (olderRows != null && olderRows > 0) {
			throw new IllegalStateException("No timestamp fully covered by the " + selection.basinCount
					+ " selected basins for var_id " + varId + " in the " + maxLookbackMillis / 3_600_000L
					+ " hours before " + maxTs + ": some basin is probably never computed.");
		}
		return -1;
	}

	/**
	 * Deletes every {@code BASINDATA} row for {@code varId} at a timestamp where
	 * not all basins of the {@code basin} table have a valid value - leftovers from
	 * a previous run that was interrupted partway through writing a timestamp (or
	 * that failed to interpolate some basins). Left in place, they would collide
	 * with that same timestamp being recomputed, since
	 * {@code (ts, basin_id, var_id)} is the table's primary key and the daily job
	 * only appends ({@code doOverwrite=false}). Does nothing if there are no
	 * basins, so an empty/missing {@code basin} table never wipes the data.
	 */
	public final static void deletePartiallyCoveredBasinData(ASpatialDb db, int varId, Collection<Integer> basinIds)
			throws Exception {
		String tableName = GeoFrameSimpleTable.BASINDATA.tableName();
		var selection = resolveBasinSelection(db, basinIds);

		if (selection.basinCount == 0 || !db.hasTable(tableName)) {
			return;
		}
		db.executeInsertUpdateDeleteSql("DELETE FROM " + tableName + " WHERE " + VAR_ID + " = " + varId + " AND " + TS
				+ " IN (SELECT " + TS + " FROM " + tableName + " WHERE " + VAR_ID + " = " + varId + " GROUP BY " + TS
				+ " HAVING COUNT(DISTINCT CASE WHEN " + IS_VALID_VALUE_SQL + " AND "
				+ basinFilterSql(selection.basinIds) + " THEN " + BASIN_ID + " END) < " + selection.basinCount + ")");
	}

	/**
	 * delete rows
	 */
	public final static void deleteData(ASpatialDb db, String tableName, String where) throws Exception {
		if (!db.hasTable(tableName)) {
			return;
		}
		db.executeInsertUpdateDeleteSql("DELETE FROM " + tableName + " WHERE " + where);
	}

	/**
	 * Number of distinct stations reporting a valid (non-novalue) value for
	 * {@code varId} at {@code ts} in {@code station_data}.
	 */
	public final static long countStationsWithValidValue(ASpatialDb db, long ts, int varId) throws Exception {
		String tableName = GeoFrameSimpleTable.STATIONDATA.tableName();
		if (!db.hasTable(tableName)) {
			return 0;
		}
		Long count = db.getLong("SELECT COUNT(DISTINCT " + StationDataField.STATION_ID.columnName() + ") FROM "
				+ tableName + " WHERE " + StationDataField.TS.columnName() + " = " + ts + " AND "
				+ StationDataField.VAR_ID.columnName() + " = " + varId + " AND " + IS_VALID_VALUE_SQL);
		return count == null ? 0 : count;
	}

	/**
	 * Number of distinct stations reporting a valid (non-novalue) value for
	 * {@code varId} at each timestamp between {@code fromTs} and {@code toTs}
	 * (inclusive) in {@code station_data}. Timestamps without any valid value are
	 * missing from the map.
	 * <p>
	 * One query for the whole range: calling
	 * {@link #countStationsWithValidValue(ASpatialDb, long, int)} for each hour
	 * scans the whole table every time, since its primary key starts with
	 * {@code station_id}.
	 * </p>
	 */
	public final static Map<Long, Long> countStationsWithValidValue(ASpatialDb db, long fromTs, long toTs, int varId)
			throws Exception {
		Map<Long, Long> counts = new HashMap<>();
		String tableName = GeoFrameSimpleTable.STATIONDATA.tableName();
		if (!db.hasTable(tableName)) {
			return counts;
		}
		String tsColumn = StationDataField.TS.columnName();
		QueryResult result = db.getTableRecordsMapFromRawSql("SELECT " + tsColumn + ", COUNT(DISTINCT "
				+ StationDataField.STATION_ID.columnName() + ") AS n FROM " + tableName + " WHERE " + tsColumn
				+ " BETWEEN " + fromTs + " AND " + toTs + " AND " + StationDataField.VAR_ID.columnName() + " = " + varId
				+ " AND " + IS_VALID_VALUE_SQL + " GROUP BY " + tsColumn, -1);
		int tsIndex = result.names.indexOf(tsColumn);
		int countIndex = result.names.indexOf("n");
		for (Object[] row : result.data) {
			counts.put(((Number) row[tsIndex]).longValue(), ((Number) row[countIndex]).longValue());
		}
		return counts;
	}

	/**
	 * Returns the basins among {@code basinIds} whose {@code varId} series is not
	 * complete between {@code fromTsMillis} and {@code toTsMillis} (inclusive),
	 * i.e. that miss a valid (non-novalue) value at some step of
	 * {@code stepMillis}, with a single query for all the basins.
	 *
	 * @return the incomplete basin ids, empty if every series is complete
	 */
	public static List<Integer> findBasinsWithIncompleteSeries(ASpatialDb db, int varId, long fromTsMillis,
			long toTsMillis, long stepMillis, Collection<Integer> basinIds) throws Exception {

		Objects.requireNonNull(db, "db");
		Objects.requireNonNull(basinIds, "basinIds");

		if (stepMillis <= 0) {
			throw new IllegalArgumentException("stepMillis must be greater than zero");
		}

		if (toTsMillis < fromTsMillis) {
			throw new IllegalArgumentException("toTsMillis must be greater than or equal to fromTsMillis");
		}

		long intervalMillis = toTsMillis - fromTsMillis;

		if (intervalMillis % stepMillis != 0) {
			throw new IllegalArgumentException("The requested interval is not aligned with the timestep: " + "from="
					+ fromTsMillis + ", to=" + toTsMillis + ", step=" + stepMillis);
		}

		var selection = resolveBasinSelection(db, basinIds);

		if (selection.basinCount == 0) {
			return List.of();
		}

		String tableName = GeoFrameSimpleTable.BASINDATA.tableName();

		if (!db.hasTable(tableName)) {
			return new ArrayList<>(selection.basinIds);
		}

		long expectedCount = Math.addExact(intervalMillis / stepMillis, 1);

		// (ts, basin_id, var_id) is the primary key: COUNT(*) per basin already counts
		// distinct timestamps, without the temp b-tree of a COUNT(DISTINCT)

		String sql = "SELECT " + BASIN_ID + ", COUNT(*) AS n" + " FROM " + tableName + " WHERE "
				+ VAR_ID + " = " + varId + " AND " + TS + " BETWEEN " + fromTsMillis + " AND " + toTsMillis + " AND (("
				+ TS + " - " + fromTsMillis + ") % " + stepMillis + ") = 0" + " AND " + IS_VALID_VALUE_SQL + " AND "
				+ basinFilterSql(selection.basinIds) + " GROUP BY " + BASIN_ID;

		QueryResult result = db.getTableRecordsMapFromRawSql(sql, -1);

		int idIndex = result.names.indexOf(BASIN_ID);
		int countIndex = result.names.indexOf("n");

		Set<Integer> completeBasins = new HashSet<>();

		for (Object[] row : result.data) {
			int basinId = ((Number) row[idIndex]).intValue();
			long actualCount = ((Number) row[countIndex]).longValue();

			if (actualCount == expectedCount) {
				completeBasins.add(basinId);
			}
		}

		return selection.basinIds.stream().filter(basinId -> !completeBasins.contains(basinId)).toList();
	}

	/**
	 * Deletes every {@code BASINDATA} row for {@code varId} after
	 * {@code lastFullyCoveredTs}, i.e. the partially covered tail left by an
	 * interrupted run, so it can be recomputed without colliding on the
	 * {@code (ts, basin_id, var_id)} primary key.
	 */
	public static void deletePartiallyCoveredBasinData(ASpatialDb db, int varId, long lastFullyCoveredTs)
			throws Exception {
		String tableName = GeoFrameSimpleTable.BASINDATA.tableName();
		if (!db.hasTable(tableName)) {
			return;
		}
		db.executeInsertUpdateDeleteSql("DELETE FROM " + tableName + " WHERE " + VAR_ID + " = " + varId + " AND " + TS
				+ " > " + lastFullyCoveredTs);
	}

}
