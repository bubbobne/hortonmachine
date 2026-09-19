package org.hortonmachine.hmachine.modules.statistics.kriging.validation;

import org.hortonmachine.gears.libs.modules.HMConstants;
import org.hortonmachine.hmachine.modules.statistics.kriging.primarylocation.StationsSelection;

/**
 * Kriging output validator that checks if values fall within specified minimum
 * and maximum bounds.
 * <p>
 * Evaluates whether a predicted Kriging output or variance is valid by
 * verifying that it is not a nodata value (according to
 * {@link HMConstants#isNovalue(double)}) and stays within the range
 * [{@code minValue}, {@code maxValue}].
 * </p>
 * 
 * @author Daniele Andreis, Giuseppe Formetta, Andrea Antonello
 */
public class MinMaxKrigingOutputValidator extends AbstractKrigingOutputValidator {
	private double maxValue = Double.MAX_VALUE;
	private double minValue = Double.MIN_VALUE;
	protected DoubleSupplierForValidator supplier;

	/**
	 * Constructs a validator with custom minimum and maximum allowed values.
	 *
	 * @param minValue          the lower bound threshold (inclusive)
	 * @param maxValue          the upper bound threshold (inclusive)
	 * @param stationsSelection the station to get values.
	 */
	public MinMaxKrigingOutputValidator(double minValue, double maxValus, DoubleSupplierForValidator doubleSupplier) {
		this.minValue = minValue;
		this.maxValue = maxValus;
		this.supplier = doubleSupplier;
	}

	/**
	 * {@inheritDoc}
	 * <p>
	 * Checks if the Kriging output value is not a nodata value and lies within the
	 * range [{@code minValue}, {@code maxValue}].
	 * </p>
	 *
	 * @param krigingOutput the value to validate
	 * @return {@code true} if the value is valid and within bounds; {@code false}
	 *         otherwise
	 */
	@Override
	public boolean validate(double krigingOutput) {
		return !HMConstants.isNovalue(krigingOutput) && krigingOutput <= maxValue && minValue <= krigingOutput;
	}

	/**
	 * Computes the fallback value when validation fails.
	 * <p>
	 * Default implementation returns {@link HMConstants#doubleNovalue}. Subclasses
	 * (e.g. clamped validators) can override this method to provide alternative
	 * strategies, such as clamping to the nearest bound.
	 * </p>
	 *
	 * @param krigingOutput the invalid Kriging output
	 * @return {@link HMConstants#doubleNovalue} as the default fallback
	 */
	// to extend
	protected double computeFallback(double krigingOutput) {
		return supplier.getValue();
	}
}