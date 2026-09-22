package org.hortonmachine.hmachine.geoframe.io.database.tables.implementation;

import java.util.List;

import org.hortonmachine.hmachine.geoframe.io.database.TableUtils;
import org.hortonmachine.hmachine.geoframe.io.database.tables.definition.SimpleAbstractSchema;
import org.hortonmachine.hmachine.geoframe.io.database.tables.definition.TableField;

/**
 * Metadata schema definition for storing and retrieving spatial variogram parameters 
 * within the {@code environmental_variables} database table.
 * 
 * <p>In geostatistics and environmental data modeling, a variogram (or semivariogram) quantifies 
 * spatial autocorrelation by measuring variance as a function of distance lag. 
 * This schema store variogram parameters, structural trends, and execution metadata to Java 
 * object types for ORM layer mapping and database operations.</p>
 *
 * <h2>Table Architecture &amp; Key Design</h2>
 * <ul>
 *   <li><b>Table Name:</b> {@code variogram}</li>
 *   <li><b>Primary Key:</b> {@link VariogramField#TS} (Timestamp representing the evaluation time step)</li>
 *   <li><b>Foreign Keys:</b> None</li>
 * </ul>
 *
 * <h2>Core Geostatistical Parameters</h2>
 * <ul>
 *   <li><b>Nugget Effect ({@link VariogramField#NUGGET}):</b> Represents micro-scale variation, measurement noise, 
 *       or spatial discontinuity at zero distance (lag = 0).</li>
 *   <li><b>Sill ({@link VariogramField#SILL}):</b> The total variance limit at which the variogram flattens out, 
 *       representing the variance of spatially uncorrelated data.</li>
 *   <li><b>Range ({@link VariogramField#RANGE}):</b> The distance or time lag at which the variogram reaches the sill. 
 *       Beyond this range, spatial or temporal autocorrelation ceases to exist.</li>
 * </ul>
 *
 * <h2>Local vs. Global Evaluation Strategy</h2>
 * <p>The model evaluation scope is controlled via the {@link VariogramField#IS_GLOBAL} flag:</p>
 * <ul>
 *   <li><b>Local Evaluation ({@code IS_GLOBAL = false}):</b> Parameters are dynamically calculated at each 
 *       individual time step, offering higher local precision and adaptive fitting for non-stationary processes.</li>
 *   <li><b>Global Evaluation ({@code IS_GLOBAL = true}):</b> Parameters are fitted over the entire time series data set, 
 *       providing stable, generalized parameters across the entire domain.</li>
 * </ul>
 *
 * <h2>Trend &amp; Non-Stationarity Handling</h2>
 * <p>Environmental time series often contain non-stationary linear trends. When a trend is present 
 * ({@link VariogramField#IS_TREND} is {@code true}), the underlying signal is detrended using a linear model 
 * parameterised by {@link VariogramField#TREND_INTERCEPT} and {@link VariogramField#TREND_SLOPE} prior to fitting the variogram.</p>
 *
 * @see SimpleAbstractSchema
 * @see VariogramField
 *
 * @author Daniele Andreis
 */
public class VariogramSchema extends SimpleAbstractSchema {
	public VariogramSchema() {
		super("variogram", VariogramField.class);
	}

	public enum VariogramField implements TableField {
		TS("ts", Long.class), //
		VARIOGRAM("variogram", String.class), //
		VARIOGRAM_ID("variogram_id", Integer.class), //
		NUGGET("nugget", Double.class), //
		SILL("sill", Double.class), //
		RANGE("range", Double.class), //
		IS_GLOBAL("is_global", Boolean.class), //
		IS_TREND("is_trend", Boolean.class), TREND_INTERCEPT("trend_intercept", Double.class),
		TREND_SLOPE("trend_slope", Double.class);

		private final String columnName;
		private final Class<?> javaType;

		VariogramField(String columnName, Class<?> javaType) {
			this.columnName = columnName;
			this.javaType = javaType;
		}

		public String columnName() {
			return columnName;
		}

		public Class<?> javaType() {
			return javaType;
		}

	}

	@Override
	protected List<TableField> primaryKey() {
		// TODO Auto-generated method stub
		return List.of(VariogramField.TS);
	}

	@Override
	protected List<ForeignKey> foreignKeys() {
		// TODO Auto-generated method stub
		return null;
	}

}