package org.hortonmachine.hmachine.geoframe.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.hortonmachine.dbs.compat.ADb;
import org.hortonmachine.dbs.compat.objects.QueryResult;
import org.hortonmachine.hmachine.geoframe.core.TopologyNode;
import org.hortonmachine.hmachine.geoframe.io.database.tables.GeoFrameSimpleTable;
import org.hortonmachine.hmachine.geoframe.io.database.tables.implementation.TopologySchema.TopologyField;

public class TopologyUtilities {
	/**
	 * Builds the basin topology from the database and returns its root node.
	 * <p>
	 * Each row of the topology table defines a connection between an upstream
	 * basin and its downstream basin. 
	 * </p>
	 * <p>
	 * This method assumes that the topology table describes a non-empty,
	 * connected and acyclic basin network with a single root.
	 * </p>
	 *
	 * @param db the database containing the topology table
	 * @return the root node of the basin topology
	 * @throws Exception if the topology table cannot be queried or processed
	 * @throws java.util.NoSuchElementException if the topology table is empty
	 */
	public static TopologyNode getRootNodeFromDb(ADb db) throws Exception {
		QueryResult result = db
				.getTableRecordsMapFromRawSql("select * from " + GeoFrameSimpleTable.TOPOLOGY.tableName(), -1);
		int fromIndex = result.names.indexOf(TopologyField.UPPSTREAM_BASIN.columnName());
		int toIndex = result.names.indexOf(TopologyField.DOWNSTREAM_BASIN.columnName());

		HashMap<Integer, TopologyNode> topologyBasinsMap = new HashMap<>();
		for (Object[] row : result.data) {
			int fromBasinId = ((Number) row[fromIndex]).intValue();
			int toBasinId = ((Number) row[toIndex]).intValue();

			TopologyNode fromNode = topologyBasinsMap.get(fromBasinId);
			if (fromNode == null) {
				fromNode = new TopologyNode(fromBasinId);
				topologyBasinsMap.put(fromBasinId, fromNode);
			}
			if (toBasinId != 0) { // 0 is used to indicate no downstream basin
				TopologyNode toNode = topologyBasinsMap.get(toBasinId);
				if (toNode == null) {
					toNode = new TopologyNode(toBasinId);
					topologyBasinsMap.put(toBasinId, toNode);
				}
				fromNode.setDownStreamNode(toNode);
			}
		}
		TopologyNode rootNode = TopologyNode.getRootNode(topologyBasinsMap.values().stream().findFirst().get());
		return rootNode;
	}

	/**
	 * Retrieves the unique identifiers of all basins included in the topology.
	 * <p>
	 * Basin identifiers are collected from both the upstream and downstream basin
	 * columns. A downstream identifier equal to {@code 0} is excluded, as it
	 * indicates that no downstream basin exists (the outlet).
	 * </p>
	 *
	 * @param db the database containing the topology table
	 * @return a list containing the unique basin id identifiers; the order is not
	 *         guaranteed due that {@link java.util.HashSet} is used
	 */
	public static ArrayList<Integer> getTopologyBasinIdList(ADb db) throws Exception {
		QueryResult result = db
				.getTableRecordsMapFromRawSql("select * from " + GeoFrameSimpleTable.TOPOLOGY.tableName(), -1);
		int fromIndex = result.names.indexOf(TopologyField.UPPSTREAM_BASIN.columnName());
		int toIndex = result.names.indexOf(TopologyField.DOWNSTREAM_BASIN.columnName());

		Set<Integer> topologyBasinsSet = new HashSet<Integer>();
		for (Object[] row : result.data) {
			int fromBasinId = ((Number) row[fromIndex]).intValue();
			int toBasinId = ((Number) row[toIndex]).intValue();
			topologyBasinsSet.add(fromBasinId);
			if (toBasinId != 0) {
				topologyBasinsSet.add(toBasinId);
			}
		}
		return new ArrayList<Integer>(topologyBasinsSet);
	}
}