package org.hortonmachine.hmachine.geoframe.io.database.tables.implementation;

import org.hortonmachine.hmachine.geoframe.io.database.tables.definition.GeoAbstractSchema;
import org.hortonmachine.hmachine.geoframe.io.database.tables.definition.TableField;
import org.locationtech.jts.geom.Point;

/**
 * Represents the schema for the monitoring station table or entity (e.g.,
 * weather or stream gauge stations). Extends {@link GeoAbstractSchema} to
 * provide spatial and geographic functionality.
 *
 * @author Daniele Andreis
 */

public class StationSchema extends GeoAbstractSchema {

	/**
	 * Constructs a new station schema, binding it to the "station" table name and
	 * the {@link Station} entity class.
	 */
	public StationSchema() {
		super("station", Station.class);
	}

	/**
	 * Defines the supported types of monitoring stations within the system.
	 */
	public enum StationType {
		/** Weather/meteorological station. */
		METEO,
		/** Stream gauge or water level measurement station. */
		STREAM_GAUGE;
	}

	/**
	 * Maps the fields and columns of the station database table, associating each
	 * enum element with its database column name and Java data type.
	 */
	public enum Station implements TableField {
		/** Geographical geometry of the station, represented as a point. */
		GEOM("the_geom", Point.class),

		/** Primary unique identifier of the station in the database. */
		ID("id", Integer.class),

		/** Actual/external identifier assigned by the data provider. */
		ACTUAL_ID("actual_id", String.class),

		/** Name of the organization or provider supplying the station data. */
		PROVIDER("provider", String.class),

		/** Display name of the station. */
		NAME("name", String.class),

		/** Elevation above sea level, in meters. */
		ELEVATION("elevation", Double.class),

		/** Identifier of the associated hydrographic basin. */
		BASIN_ID("basin_id", Integer.class),

		/** Type of the station (corresponds to {@link StationType}). */
		TYPE("type", String.class),

		/**
		 * Indicates whether the station is active.
		 * <p>
		 * Currently used in spatial interpolation algorithms (Kriging) to determine
		 * whether to include or exclude the station from calculations.
		 * </p>
		 */
		ACTIVE("active", Boolean.class)

		;

		private final String columnName;
		private final Class<?> javaType;

		/**
		 * Constructs a table field definition.
		 *
		 * @param columnName the corresponding column name in the database
		 * @param javaType   the Java class representing the field's data type
		 */
		Station(String columnName, Class<?> javaType) {
			this.columnName = columnName;
			this.javaType = javaType;
		}

		/**
		 * Returns the database column name associated with this field.
		 *
		 * @return the column name
		 */
		public String columnName() {
			return this.columnName;
		}

		/**
		 * Returns the Java data type associated with this field.
		 *
		 * @return the Java class type
		 */
		public Class<?> javaType() {
			return this.javaType;
		}
	}
}