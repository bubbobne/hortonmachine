package org.hortonmachine.hmachine.modules.statistics.kriging.validation;

public abstract class AbstractKrigingOutputValidator implements IKrigingOutputValidator {

	@Override
	public double ensureValid(double krigingOutput, double fallback) {
		return validate(krigingOutput) ? krigingOutput : fallback;
	}

	@Override
	public double getValidValue(double krigingOutput) {
		if (validate(krigingOutput)) {
			return krigingOutput;
		}
		return computeFallback(krigingOutput);
	}

	/**
	 * Hook to implements.
	 */
	protected abstract double computeFallback(double krigingOutput);
}