package org.hortonmachine.hmachine.modules.statistics.kriging.validation;

import java.util.function.DoubleSupplier;

import org.hortonmachine.gears.libs.modules.HMConstants;
import org.hortonmachine.hmachine.modules.statistics.kriging.primarylocation.StationsSelection;

/**
 * Supplier that provides the value of the nearest station from a
 * {@link StationsSelection}.
 * <p>
 * Implements {@link DoubleSupplier} so it can be passed directly to validators
 * without tightly coupling them to the {@link StationsSelection} class.
 * </p>
 * 
 * @author Daniele Andreis, Giuseppe Formetta, Andrea Antonello
 */
public class NearestStationSupplierValue implements DoubleSupplierForValidator, DoubleSupplier {
	StationsSelection stationSelection = null;

	/**
	 * Constructs the supplier wrapping a {@link StationsSelection} instance.
	 * 
	 * @param stationSelection the station selection object updated during Kriging
	 *                         execution
	 */
	public NearestStationSupplierValue(StationsSelection stationSelection) {
		this.stationSelection = stationSelection;
	}

	/**
	 * Gets the current value of the nearest station (first element in the initial
	 * set).
	 *
	 * @return the value of the nearest station, or
	 *         {@link HMConstants#doubleNovalue} if unavailable
	 */
	public double getValue() {
		if (stationSelection != null && stationSelection.hStationInitialSet.length > 0) {
			return stationSelection.hStationInitialSet[0];
		}
		return HMConstants.doubleNovalue;
	}

	/**
	 * Implementation of {@link DoubleSupplier#getAsDouble()}. Automatically calls
	 * {@link #getValue()} when evaluated.
	 */
	@Override
	public double getAsDouble() {
		return getValue();
	}

}
