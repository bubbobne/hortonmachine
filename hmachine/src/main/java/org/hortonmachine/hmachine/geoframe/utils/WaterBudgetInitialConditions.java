package org.hortonmachine.hmachine.geoframe.utils;

import java.util.Arrays;

/**
 * Holds the six per-basin state arrays a {@link org.hortonmachine.hmachine.geoframe.core.WaterBudgetSimulation}
 * needs as initial conditions, indexed by basin id (arrays are sized
 * {@code maxBasinId + 1} to match {@code TopologyNode.basinId} indexing used
 * elsewhere in the water budget model).
 *
 * <p>Used to resume a simulation from a previously persisted state (see
 * {@link WaterBudgetState#loadLastState(org.hortonmachine.dbs.compat.ASpatialDb, String, int)})
 * instead of the zero/NaN defaults {@link WaterSimulationRunner} used to
 * hardcode for every run.</p>
 */
public class WaterBudgetInitialConditions {
	public final double[] solidWater;
	public final double[] liquidWater;
	public final double[] canopy;
	public final double[] rootzone;
	public final double[] runoff;
	public final double[] ground;

	public WaterBudgetInitialConditions(double[] solidWater, double[] liquidWater, double[] canopy,
			double[] rootzone, double[] runoff, double[] ground) {
		this.solidWater = solidWater;
		this.liquidWater = liquidWater;
		this.canopy = canopy;
		this.rootzone = rootzone;
		this.runoff = runoff;
		this.ground = ground;
	}

	/**
	 * The defaults every simulation used to start from before warm-starting was
	 * supported: solid/liquid water at 0, everything else unknown (NaN).
	 */
	public static WaterBudgetInitialConditions zero(int maxBasinId) {
		double[] solidWater = new double[maxBasinId + 1];
		double[] liquidWater = new double[maxBasinId + 1];
		double[] canopy = new double[maxBasinId + 1];
		Arrays.fill(canopy, Double.NaN);
		double[] rootzone = new double[maxBasinId + 1];
		Arrays.fill(rootzone, Double.NaN);
		double[] runoff = new double[maxBasinId + 1];
		Arrays.fill(runoff, Double.NaN);
		double[] ground = new double[maxBasinId + 1];
		Arrays.fill(ground, Double.NaN);
		return new WaterBudgetInitialConditions(solidWater, liquidWater, canopy, rootzone, runoff, ground);
	}
}
