package org.hortonmachine.hmachine.modules.statistics.kriging.validation;

/**
 * Interface to validate and adjust Kriging output predictions or variances.
 * <p>
 * Implementations of this interface can check whether a generated Kriging value
 * complies with domain constraints (e.g., physical limits, non-negativity) and
 * provide fallback strategies when an invalid value (such as NaN or an
 * out-of-bounds number) is encountered.
 * </p>
 * 
 * <p>
 * N.B.: this interface should be use in a real-time simulation, but it should
 * be logged if the fallback value is used.
 * </p>
 * 
 * @author Daniele Andreis, Giuseppe Formetta, Andrea Antonello
 */
public interface IKrigingOutputValidator {
	/**
	 * Evaluates whether the given Kriging output value satisfies the validation
	 * criteria.
	 *
	 * @param krigingOutput the interpolated value or variance produced by the
	 *                      Kriging algorithm
	 * @return {@code true} if the output is valid; {@code false} otherwise (e.g.
	 *         NaN, infinite, or out of bounds)
	 */
	boolean validate(double krigingOutput);

	/**
	 * Returns the original Kriging output if valid, or a specified fallback value
	 * if invalid.
	 *
	 * @param krigingOutput the interpolated value or variance to test
	 * @param fallback      the custom value to return if {@code krigingOutput} is
	 *                      invalid
	 * @return {@code krigingOutput} if valid; {@code fallback} otherwise
	 */
	double ensureValid(double krigingOutput, double fallback);

	/**
	 * Returns the original Kriging output if valid, or an internally
	 * calculated/configured valid replacement value (e.g. a clamped bound or
	 * default fallback value).
	 *
	 * @param krigingOutput the interpolated value or variance to validate and
	 *                      resolve
	 * @return the original valid value or the default replacement value
	 */
	double getValidValue(double krigingOutput);
}
