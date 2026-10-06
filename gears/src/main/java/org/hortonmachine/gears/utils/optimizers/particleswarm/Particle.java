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

import org.hortonmachine.gears.utils.math.NumericsUtilities;

/**
 * 
 * Class representing a particle in the swarm.
 * 
 * @author Andrea Antonello (www.hydrologis.com)
 */
public class Particle {
    /**
     * List of locations in parameter space;
     */
    private double[] locations = null;

    /** 
     * Velocity vector 
     */
    private double[] particleVelocities;

    /** 
     * Best local positions found.
     */
    private double[] particleLocalBests;

    /** 
     * Best function value. 
     */
    private double particleBestFunction;

    private double[][] ranges;

    private double[] initialLocations;

    /**
     * Random generator of this particle, so that particles updated in parallel
     * do not share (and contend) a generator and results stay reproducible.
     */
    private Random rand;

    /**
     * Maximum velocity per iteration, as fraction of each parameter range.
     */
    private double maxVelocityFraction = PSEngine.DEFAULT_MAX_VELOCITY_FRACTION;

    /**
     * Create a new {@link Particle} with a given number of parameters dimension.
     *
     * @param ranges the parameters spaces ranges.
     */
    public Particle( double[][] ranges ) {
        this(ranges, new Random());
    }

    /**
     * Create a new {@link Particle} with a given number of parameters dimension.
     *
     * @param ranges the parameters spaces ranges.
     * @param rand the random generator to use for this particle.
     */
    public Particle( double[][] ranges, Random rand ) {
        this.ranges = ranges;
        this.rand = rand;

        /*
         * initialize random positions uniformly over the
         * whole parameter space
         */
        double[] r = new double[ranges.length];
        for( int i = 0; i < r.length; i++ ) {
            double min = ranges[i][0];
            double max = ranges[i][1];
            r[i] = min + (max - min) * rand.nextDouble();
        }

        locations = r;
        initialLocations = new double[locations.length];

        System.arraycopy(locations, 0, initialLocations, 0, r.length);

        particleLocalBests = new double[locations.length];
        particleVelocities = new double[locations.length];
        for( int i = 0; i < locations.length; i++ ) {
            // store the location
            particleLocalBests[i] = locations[i];
            // clear the velocity vector
            particleVelocities[i] = 0.0;
        }
    }
    /**
     * @return the initial swarm location.
     */
    public double[] getInitialLocations() {
        return initialLocations;
    }

    /**
     * Set the maximum velocity per iteration, as fraction of each parameter range.
     *
     * @param maxVelocityFraction the fraction (e.g. 0.2 = 20% of the range).
     */
    public void setMaxVelocityFraction( double maxVelocityFraction ) {
        this.maxVelocityFraction = maxVelocityFraction;
    }

    /**
     * Particle swarming formula to update positions.
     *
     * <p>
     * Random factors are drawn independently for each dimension, the velocity is
     * clamped to {@link #maxVelocityFraction} of the range and the ranges act as
     * absorbing walls: a location falling outside is set on the boundary and its
     * velocity in that dimension is zeroed, so the particle is always evaluated.
     * </p>
     *
     * @param w inertia weight (controls the impact of the past velocity of the
     *              particle over the current one).
     * @param c1 constant weighting the influence of local best
     *              solutions.
     * @param c2 constant weighting the influence of global best
     *              solutions.
     * @param globalBest leader particle (global best) in all dimensions.
     * @return the updated locations.
     */
    public double[] update( double w, double c1, double c2, double[] globalBest ) {
        for( int i = 0; i < locations.length; i++ ) {
            double min = ranges[i][0];
            double max = ranges[i][1];
            double maxVelocity = maxVelocityFraction * (max - min);

            double velocity = w * particleVelocities[i] + //
                    c1 * rand.nextDouble() * (particleLocalBests[i] - locations[i]) + //
                    c2 * rand.nextDouble() * (globalBest[i] - locations[i]);
            velocity = Math.max(-maxVelocity, Math.min(maxVelocity, velocity));

            double location = locations[i] + velocity;
            if (location < min) {
                location = min;
                velocity = 0.0;
            } else if (location > max) {
                location = max;
                velocity = 0.0;
            }
            particleVelocities[i] = velocity;
            locations[i] = location;
        }
        return locations;
    }

    /**
     * Calculated local best function value for the particle.
     * 
     * @return the local best function value for the particle.
     */
    public double getParticleBestFunction() {
        return particleBestFunction;
    }

    /**
     * Setter for the local best function value of the particle.
     * 
     * @param particleBestFunction the new local best function value to set for the particle.
     */
    public void setParticleBestFunction( double particleBestFunction ) {
        this.particleBestFunction = particleBestFunction;
    }

    /**
     * Setter to set the current positions to be the local best positions.
     */
    public void setParticleLocalBeststoCurrent() {
        for( int i = 0; i < locations.length; i++ ) {
            particleLocalBests[i] = locations[i];
        }
    }
}