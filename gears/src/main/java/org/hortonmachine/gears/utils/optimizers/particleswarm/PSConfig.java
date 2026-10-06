package org.hortonmachine.gears.utils.optimizers.particleswarm;

public class PSConfig {
	public int particlesNum = 5;
	public int maxIterations = 100;
	public double c1 = 2.0;
	public double c2 = 2.0;
	public double w0 = 0.9;
	public double decay = 0.4;
	/**
	 * Seed of the random generators, <code>null</code> for a random seed.
	 */
	public Long seed = null;
}
