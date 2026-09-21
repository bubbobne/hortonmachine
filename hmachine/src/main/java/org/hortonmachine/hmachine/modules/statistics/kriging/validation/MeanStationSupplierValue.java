package org.hortonmachine.hmachine.modules.statistics.kriging.validation;

import java.util.Arrays;
import java.util.function.DoubleSupplier;

import org.hortonmachine.gears.libs.modules.HMConstants;
import org.hortonmachine.hmachine.modules.statistics.kriging.primarylocation.StationsSelection;

/**
 * Supplier that provides the value of the mean value from station from a
 * {@link StationsSelection}.
 * <p>
 * Implements {@link DoubleSupplier} so it can be passed directly to validators
 * without tightly coupling them to the {@link StationsSelection} class.
 * </p>
 * 
 * @author Daniele Andreis, Giuseppe Formetta, Andrea Antonello
 */
public class MeanStationSupplierValue extends NearestStationSupplierValue {

	public MeanStationSupplierValue(StationsSelection stationSelection) {
		super(stationSelection);
	}

	public double getValue() {
		if (stationSelection != null && stationSelection.hStationInitialSet.length > 0) {
			return Arrays.stream(stationSelection.hStationInitialSet)
					.filter(v -> !HMConstants.isNovalue(v) && !Double.isNaN(v)).average()
					.orElse(HMConstants.doubleNovalue);
		}
		return HMConstants.doubleNovalue;
	}

}
