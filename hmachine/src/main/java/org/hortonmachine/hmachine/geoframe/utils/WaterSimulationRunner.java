package org.hortonmachine.hmachine.geoframe.utils;

import java.util.Arrays;

import org.hortonmachine.dbs.compat.ADb;
import org.hortonmachine.gears.libs.monitor.DummyProgressMonitor;
import org.hortonmachine.gears.libs.monitor.IHMProgressMonitor;
import org.hortonmachine.hmachine.geoframe.calibration.WaterBudgetParameters;
import org.hortonmachine.hmachine.geoframe.core.TopologyNode;
import org.hortonmachine.hmachine.geoframe.core.WaterBudgetSimulation;
import org.hortonmachine.hmachine.geoframe.io.GeoframeEnvDatabaseIterator;
import org.hortonmachine.hmachine.geoframe.io.GeoframeWaterBudgetSimulationWriter;

public class WaterSimulationRunner implements IWaterBudgetSimulationRunner {

	private int timeStepMinutes;
	private int maxBasinId;
	private TopologyNode rootNode;
	private double[] basinAreas;
	private ADb outputDb;
	private IHMProgressMonitor pm;
	private boolean doTopologicallyOrdered;
	private boolean doParallel;
	private boolean writeState = false;
	private String dischargeTableName;
	private String stateTableName;

	@Override
	public void configure(int timeStepMinutes, int maxBasinId, TopologyNode rootNode, double[] basinAreas,
			boolean doParallel, boolean doTopologicallyOrdered, boolean writeState, ADb outputDb,
			IHMProgressMonitor pm, String dischargeTableName, String stateTableName) {
		this.timeStepMinutes = timeStepMinutes;
		this.maxBasinId = maxBasinId;
		this.rootNode = rootNode;
		this.basinAreas = basinAreas;
		this.doParallel = doParallel;
		this.doTopologicallyOrdered = doTopologicallyOrdered;
		this.writeState = writeState;
		this.outputDb = outputDb;
		this.pm = pm;
		this.dischargeTableName = dischargeTableName;
		this.stateTableName = stateTableName;
	}

	@Override
	public double[] run(WaterBudgetParameters wbParams, double lai, GeoframeEnvDatabaseIterator precipReader,
			GeoframeEnvDatabaseIterator tempReader, GeoframeEnvDatabaseIterator etpReader, String iterationInfo,
			WaterBudgetInitialConditions initialConditions) throws Exception {
		TopologyNode localRootNode = rootNode.clone();
		
		if (pm == null) {
			pm = new DummyProgressMonitor();
		}
		long t1 = 0;
		if (iterationInfo != null) {
			t1 = System.currentTimeMillis();
			pm.message("Begin: " + iterationInfo);
		}

		GeoframeWaterBudgetSimulationWriter resultsWriter = null;
		if (outputDb != null) {
			resultsWriter = new GeoframeWaterBudgetSimulationWriter();
			resultsWriter.db = outputDb;
			resultsWriter.rootNode = localRootNode;
			if (dischargeTableName != null) {
				resultsWriter.tableName = dischargeTableName;
			}
		}

		if (initialConditions == null) {
			initialConditions = WaterBudgetInitialConditions.zero(maxBasinId);
		}

		WaterBudgetSimulation wbSim = new WaterBudgetSimulation();
		wbSim.pm = pm;
		wbSim.rootNode = localRootNode;
		wbSim.basinAreas = basinAreas;
		wbSim.timeStepMinutes = timeStepMinutes;
		wbSim.precipReader = precipReader;
		wbSim.tempReader = tempReader;
		wbSim.etpReader = etpReader;
		wbSim.initialConditionSolidWater = initialConditions.solidWater;
		wbSim.initialConditionLiquidWater = initialConditions.liquidWater;
		wbSim.initalConditionsCanopyMap = initialConditions.canopy;
		wbSim.initalConditionsRootzoneMap = initialConditions.rootzone;
		wbSim.initalConditionsRunoffMap = initialConditions.runoff;
		wbSim.initalConditionsGroundMap = initialConditions.ground;
		wbSim.wbSimParams = wbParams;
		wbSim.lai = lai;
		wbSim.resultsWriter = resultsWriter;
		wbSim.doParallel = doParallel;
		wbSim.doTopologically = doTopologicallyOrdered;
		wbSim.doDebugMessages = outputDb != null;
		wbSim.stateDb = writeState ? outputDb : null;
		wbSim.stateTableName = stateTableName;

		wbSim.init();
		wbSim.process();

		var lastNodeDischargeArray = wbSim.outRootNodeDischargeInTime;
		var sim = lastNodeDischargeArray.getTrimmedInternalArray();

		if (iterationInfo != null) {
			long t2 = System.currentTimeMillis();
			pm.message("End " + iterationInfo + " (" + (t2 - t1) / 1000.0 + " seconds)");
		}
		return sim;
	}


}