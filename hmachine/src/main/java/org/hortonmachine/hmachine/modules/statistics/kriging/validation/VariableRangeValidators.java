package org.hortonmachine.hmachine.modules.statistics.kriging.validation;

import org.hortonmachine.hmachine.geoframe.io.database.tables.implementation.VariableSchema.EnvironmentalVariableType;
import org.hortonmachine.hmachine.geoframe.io.database.tables.implementation.VariableSchema.TimeResolution;

/**
 * Single source of the plausible [min, max] range of each environmental
 * variable, used both on the aggregated station data and on the kriging output.
 * <p>
 * Ranges depend on the time resolution only for the accumulated variables
 * (precipitation, evapotranspiration); temperature and radiation are means over
 * the time step, so their range is the same.
 * </p>
 * <ul>
 * <li>temperature [&deg;C]: -40 / 50</li>
 * <li>precipitation [mm]: hourly 0 / 80, daily 0 / 400</li>
 * <li>radiation [W/m2]: hourly 0 / 1500, daily 0 / 500</li>
 * <li>evapotranspiration [mm]: hourly 0 / 2, daily 0 / 15</li>
 * </ul>
 *
 * @author Daniele Andreis, Giuseppe Formetta, Andrea Antonello
 */
public class VariableRangeValidators {

	private VariableRangeValidators() {
	}

	/**
	 * @param type       the variable
	 * @param resolution the time step of the values to validate
	 * @return a validator for the plausible range, falling back to novalue
	 * @throws IllegalArgumentException if no range is defined for the
	 *                                  variable/resolution
	 */
	public static MinMaxKrigingOutputValidator forVariable(EnvironmentalVariableType type, TimeResolution resolution) {
		double[] range = range(type, resolution);
		return new MinMaxKrigingOutputValidator(range[0], range[1]);
	}

	/**
	 * @param type       the variable
	 * @param resolution the time step of the values to validate
	 * @return {@code [min, max]}, both inclusive
	 * @throws IllegalArgumentException if no range is defined for the
	 *                                  variable/resolution
	 */
	public static double[] range(EnvironmentalVariableType type, TimeResolution resolution) {
		if (resolution != TimeResolution.HOURLY && resolution != TimeResolution.DAILY) {
			throw new IllegalArgumentException("No validation range defined for time resolution " + resolution);
		}
		boolean hourly = resolution == TimeResolution.HOURLY;
		return switch (type) {
		case TEMPERATURE -> new double[] { -40, 50 };
		case PRECIPITATION -> hourly ? new double[] { 0, 80 } : new double[] { 0, 400 };
		case RADIATION -> hourly ? new double[] { 0, 2000 } : new double[] { 0, 6500 };
		case EVAPOTRANSPIRATION -> hourly ? new double[] { 0, 2 } : new double[] { 0, 15 };
		default -> throw new IllegalArgumentException("No validation range defined for variable " + type);
		};
	}
}
