/*
 * This file is part of HortonMachine (http://www.hortonmachine.org)
 * (C) HydroloGIS - www.hydrologis.com 
 * 
 * The HortonMachine is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.hortonmachine.gears.utils.optimizers.particleswarm;

import java.util.Arrays;
import java.util.Random;

import org.hortonmachine.gears.libs.exceptions.ModelsIllegalargumentException;
import org.hortonmachine.gears.libs.monitor.IHMProgressMonitor;
import org.hortonmachine.gears.utils.math.NumericsUtilities;

/**
 * Particle swarm main engine.
 *
 * <p>
 * http://www.borgelt.net/psopt.html
 * <p>
 * Biblio: http://ncra.ucd.ie/COMP30290/crc2006/Olapeju_Ayoola_03304281.pdf ?
 * </p>
 * 
 * @author Andrea Antonello (www.hydrologis.com)
 */
public class PSEngine {

	/**
	 * Default maximum velocity per iteration, as fraction of each parameter range.
	 */
	public static final double DEFAULT_MAX_VELOCITY_FRACTION = 0.2;

	private double accelerationFactorLocal;
	private double accelerationFactorGlobal;
	private double initDecelerationFactor;
	private int maxIterations;
	private double decayFactor;
	private int particlesNum;
	private Particle[] swarm;
	private double globalBestCost;
	private double[] globalBestLocations;
	private IPSFunction function;
	private int iterationStep;
	private Random rand;
	private double[][] ranges;
	private String prefix;

	private final Object globalLock = new Object(); // lock for global best updates
	private Integer numOfThreads;
	private IHMProgressMonitor pm;
	private boolean printDebug = false;
	private Long seed = null;
	private double maxVelocityFraction = DEFAULT_MAX_VELOCITY_FRACTION;

	/**
	 * Constructor.
	 * 
	 * @param particlesNum             number of particles involved.
	 * @param maxIterations            maximum iterations.
	 * @param accelerationFactorLocal  local acceleration factor for particles.
	 * @param accelerationFactorGlobal global acceleration factor for particles.
	 * @param initDecelerationFactor   initial deceleration factor.
	 * @param decayFactor              decay factor.
	 * @param function                 the fitting {@link IPSFunction function} to
	 *                                 use.
	 * @param prefix                   TODO
	 */
	public PSEngine(int particlesNum, int maxIterations, double accelerationFactorLocal,
			double accelerationFactorGlobal, double initDecelerationFactor, double decayFactor, IPSFunction function,
			Integer numOfThreads, String prefix, IHMProgressMonitor pm) {
		this.particlesNum = particlesNum;
		this.accelerationFactorLocal = accelerationFactorLocal;
		this.accelerationFactorGlobal = accelerationFactorGlobal;
		this.initDecelerationFactor = initDecelerationFactor;
		this.decayFactor = decayFactor;
		this.maxIterations = maxIterations;
		this.function = function;
		this.numOfThreads = numOfThreads;
		this.prefix = prefix;
		this.pm = pm;
	}

	public void setPrintDebug(boolean printDebug) {
		this.printDebug = printDebug;
	}

	/**
	 * Set the seed of the random generators, to get reproducible runs.
	 *
	 * @param seed the seed, or <code>null</code> for a random seed.
	 */
	public void setSeed(Long seed) {
		this.seed = seed;
	}

	/**
	 * Set the maximum velocity per iteration of the particles.
	 *
	 * @param maxVelocityFraction the fraction of each parameter range (default
	 *                            {@link #DEFAULT_MAX_VELOCITY_FRACTION}).
	 */
	public void setMaxVelocityFraction(double maxVelocityFraction) {
		this.maxVelocityFraction = maxVelocityFraction;
	}

	/**
	 * @return the number of threads to use: the requested one, or the available
	 *         processors, never more than the particles.
	 */
	private int getThreadsNum() {
		int nThreads = Runtime.getRuntime().availableProcessors();
		if (numOfThreads != null && numOfThreads > 0) {
			nThreads = numOfThreads;
		}
		return Math.max(1, Math.min(nThreads, particlesNum));
	}

	/**
	 * Create the particles, each one with its own random generator derived from
	 * the engine one, so that the result does not depend on thread scheduling.
	 */
	private void initParticles() {
		rand = seed != null ? new Random(seed) : new Random();
		swarm = new Particle[particlesNum];
		for (int j = 0; j < swarm.length; j++) {
			swarm[j] = new Particle(ranges, new Random(rand.nextLong()));
			swarm[j].setMaxVelocityFraction(maxVelocityFraction);
		}
	}

	/**
	 * Set ranges for the parameter space.
	 * 
	 * <p>
	 * The order of the ranges needs to be the same that will be used be the
	 * particles and fitting function.
	 * 
	 * @param ranges the [min, max] ranges to use.
	 */
	public void initializeRanges(double[]... ranges) {
		this.ranges = ranges;
	}

	/**
	 * Run the particle swarm engine.
	 * 
	 * @throws Exception
	 */
	public void run() throws Exception {
		if (ranges == null) {
			throw new ModelsIllegalargumentException("No ranges have been defined for the parameter space.", this);
		}
		if (pm != null)
			pm.beginTask("Calibrating with Particle swarm...", maxIterations);
		if (numOfThreads != null && numOfThreads > 1) {
			createSwarmMultiThreaded();
		} else {
			createSwarm();
		}

		if (printDebug && pm != null) {
			pm.message(prefix + " INITIAL BEST COST = " + globalBestCost);
			pm.message(prefix + " INITIAL BEST PARAMS = " + Arrays.toString(globalBestLocations));
		}

		double[] previous = null;
		while (iterationStep <= maxIterations) {
			if (numOfThreads != null && numOfThreads > 1) {
				updateSwarmMultiThreaded();
			} else {
				updateSwarm();
			}

			if (function.hasConverged(globalBestCost, globalBestLocations, previous)) {
				break;
			}
			previous = globalBestLocations.clone();

			if (printDebug && pm != null) {
				pm.message(prefix + " CURRENT BEST COST = " + globalBestCost);
				pm.message(prefix + " CURRENT BEST PARAMS = " + Arrays.toString(globalBestLocations));
			}
			if (pm != null) {
				pm.worked(1);
				pm.message("PSCalibration iter " + iterationStep + " best cost = " + (-globalBestCost));
			}
		}
		if (printDebug && pm != null) {
			pm.message(prefix + " FINAL BEST COST = " + globalBestCost);
			pm.message(prefix + " FINAL BEST PARAMS = " + Arrays.toString(globalBestLocations));
		}
		if (pm != null)
			pm.done();

	}

	/**
	 * Getter for the found solution.
	 * 
	 * @return the solution.
	 */
	public double[] getSolution() {
		return globalBestLocations.clone();
	}

	public double getSolutionFittingValue() {
		return globalBestCost;
	}

	private void createSwarm() throws Exception {
		iterationStep = 0;
		globalBestCost = function.getInitialGlobalBest();
		initParticles();
		for (int j = 0; j < swarm.length; j++) {
			double[] currentLocations = swarm[j].getInitialLocations();
			double evaluatedCost = function.evaluateCost(iterationStep, j, currentLocations, ranges);
			swarm[j].setParticleBestFunction(evaluatedCost);
			/* find globally best function value */
			if (function.isBetter(evaluatedCost, globalBestCost)) {
				globalBestCost = evaluatedCost;
				if (globalBestLocations == null) {
					globalBestLocations = new double[currentLocations.length];
				}
				for (int k = 0; k < currentLocations.length; k++) {
					globalBestLocations[k] = currentLocations[k];
				}
			} else if (globalBestLocations == null) {
				throw new RuntimeException("No evaluated value found better than the initial global best: "
						+ evaluatedCost + " vs. " + globalBestCost);
			}
		}
	}

	private void createSwarmMultiThreaded() throws Exception {
		iterationStep = 0;
		globalBestCost = function.getInitialGlobalBest();
		initParticles();

		// globalBestLocations will be initialized lazily once we know the dimension
		globalBestLocations = null;

		java.util.concurrent.ExecutorService executor = java.util.concurrent.Executors
				.newFixedThreadPool(getThreadsNum());

		java.util.List<java.util.concurrent.Future<?>> futures = new java.util.ArrayList<>();

		for (int j = 0; j < swarm.length; j++) {
			final int particleIndex = j;

			futures.add(executor.submit(() -> {
				// evaluate the initial position of the particle
				Particle p = swarm[particleIndex];

				double[] currentLocations = p.getInitialLocations();
				double evaluatedCost = function.evaluateCost(iterationStep, particleIndex, currentLocations, ranges);
				p.setParticleBestFunction(evaluatedCost);

				// update global best (thread-safe)
				synchronized (globalLock) {
					if (function.isBetter(evaluatedCost, globalBestCost)) {
						globalBestCost = evaluatedCost;

						if (globalBestLocations == null) {
							globalBestLocations = new double[currentLocations.length];
						}
						System.arraycopy(currentLocations, 0, globalBestLocations, 0, currentLocations.length);
					}
				}

				return null;
			}));
		}

		// wait for all particles to finish initialization
		for (java.util.concurrent.Future<?> f : futures) {
			f.get();
		}
		executor.shutdown();

		// Safety check (should not happen in practice)
		if (globalBestLocations == null) {
			throw new RuntimeException(
					"No evaluated value found better than the initial global best: " + globalBestCost);
		}
	}

	private void updateSwarm() throws Exception {
		// System.out.println("UPDATE SWARM");
		iterationStep++;
		/*
		 * velocity decay factor:
		 * 
		 * - it decreases when the iteration number increases - it decreases when the
		 * decay factor increases
		 */
		double w = initDecelerationFactor * Math.pow(iterationStep, -decayFactor);
		// System.out.println("W = " + w);
		/* traverse the particles */
		for (int i = 0; i < swarm.length; i++) {
			Particle particle = this.swarm[i];
			double[] currentLocations = particle.update(w, accelerationFactorLocal, accelerationFactorGlobal,
					globalBestLocations);
			double evaluatedCost = function.evaluateCost(iterationStep, i, currentLocations, ranges);
			/* update best local function value */
			if (function.isBetter(evaluatedCost, particle.getParticleBestFunction())) {
				particle.setParticleBestFunction(evaluatedCost);
				particle.setParticleLocalBeststoCurrent();
			}
			/* update best global function value */
			if (function.isBetter(evaluatedCost, globalBestCost)) {
				globalBestCost = evaluatedCost;
				for (int j = 0; j < currentLocations.length; j++) {
					globalBestLocations[j] = currentLocations[j];
				}
			}
		}
	}

	private void updateSwarmMultiThreaded() throws Exception {
		iterationStep++;

		double w = initDecelerationFactor * Math.pow(iterationStep, -decayFactor);

		var executor = java.util.concurrent.Executors.newFixedThreadPool(getThreadsNum());

		var futures = new java.util.ArrayList<java.util.concurrent.Future<?>>();

		// all particles of this iteration move towards the same global best, read
		// from a copy so that no thread sees it while another one is writing it
		double[] globalBestSnapshot;
		synchronized (globalLock) {
			globalBestSnapshot = globalBestLocations.clone();
		}

		for (int i = 0; i < swarm.length; i++) {
			final int particleIndex = i;

			futures.add(executor.submit(() -> {
				Particle particle = swarm[particleIndex];

				double[] currentLocations = particle.update(w, accelerationFactorLocal, accelerationFactorGlobal,
						globalBestSnapshot);

				double evaluatedCost = function.evaluateCost(iterationStep, particleIndex, currentLocations, ranges);

				// update particle-local best
				if (function.isBetter(evaluatedCost, particle.getParticleBestFunction())) {
					particle.setParticleBestFunction(evaluatedCost);
					particle.setParticleLocalBeststoCurrent();
				}

				// update global best (thread-safe)
				synchronized (globalLock) {
					if (function.isBetter(evaluatedCost, globalBestCost)) {
						globalBestCost = evaluatedCost;
						System.arraycopy(currentLocations, 0, globalBestLocations, 0, currentLocations.length);
					}
				}

				return null;
			}));
		}

		// wait for all threads to finish
		for (var f : futures)
			f.get();

		executor.shutdown();
	}

	/**
	 * Checks if the parameters are in the ranges.
	 * 
	 * @param parameters the params.
	 * @param ranges     the ranges.
	 * @return <code>true</code>, if they are inside the given ranges.
	 */
	public static boolean parametersInRange(double[] parameters, double[]... ranges) {
		for (int i = 0; i < ranges.length; i++) {
			if (!NumericsUtilities.isBetween(parameters[i], ranges[i])) {
				return false;
			}
		}
		return true;
	}

}
