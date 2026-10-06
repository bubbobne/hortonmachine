package org.hortonmachine.hmachine.geoframe.ermworkflow;

import org.hortonmachine.dbs.compat.ASpatialDb;
import org.hortonmachine.dbs.compat.EDb;
import org.hortonmachine.gears.libs.modules.HMConstants;
import org.hortonmachine.gears.libs.modules.HMModel;
import org.hortonmachine.hmachine.geoframe.io.GeoframeEnvDatabaseIterator;
import org.hortonmachine.hmachine.geoframe.io.database.tables.GeoFrameGeoTable;
import org.hortonmachine.hmachine.geoframe.io.database.tables.GeoFrameSimpleTable;
import org.hortonmachine.hmachine.geoframe.io.database.tables.implementation.BasinDataSchema.BasinDataField;
import org.hortonmachine.hmachine.geoframe.io.database.tables.implementation.StationSchema;
import org.hortonmachine.hmachine.geoframe.io.database.tables.implementation.StationSchema.Station;
import org.hortonmachine.hmachine.geoframe.io.database.tables.implementation.VariableSchema;
import org.hortonmachine.hmachine.geoframe.utils.KrigingAtCentroid;
import org.hortonmachine.hmachine.modules.statistics.kriging.validation.IKrigingOutputValidator;
import org.hortonmachine.hmachine.modules.statistics.kriging.validation.VariableRangeValidators;

import oms3.annotations.Author;
import oms3.annotations.Description;
import oms3.annotations.Execute;
import oms3.annotations.In;
import oms3.annotations.Keywords;
import oms3.annotations.Label;
import oms3.annotations.License;
import oms3.annotations.Name;
import oms3.annotations.Status;
import oms3.annotations.UI;

@Description("Third step of the ERM/GeoFrame water budget workflow: interpolates the temperature and precipitation measured at the meteo stations to the centroids of the sub-basins, with ordinary kriging and a variogram fitted at each time step. Negative interpolated precipitations are set to zero.")
@Author(name = "Daniele Andreis", contact = "")
@Keywords("ERM, GeoFrame, Kriging")
@Label("GeoFrame")
@Name("ermKriging")
@Status(40)
@License("General Public License Version 3 (GPLv3)")
public class ErmKriging extends HMModel {
	@Description("The GeoFrame database, with the station data imported.")
	@UI(HMConstants.FILEIN_UI_HINT_GPKG)
	@In
	public String inGpkg;

	@Description("Start of the period to interpolate. Format: yyyy-MM-dd HH:mm, in UTC.")
	@In
	public String pStartTimestamp;

	@Description("End of the period to interpolate. Format: yyyy-MM-dd HH:mm, in UTC.")
	@In
	public String pEndTimestamp;

	@Description("If true, the temperatures and precipitations already interpolated are deleted first.")
	@In
	public boolean doDeleteExistingData = false;

	@Description("Time resolution of the station data (HOURLY or DAILY), selects the validator ranges.")
	@In
	public String pTimeResolution = "HOURLY";

	private ASpatialDb db;
	private int maxId;

	@Execute
	public void process() throws Exception {
		try {
			db = EDb.GEOPACKAGE.getSpatialDb();
			db.open(inGpkg);
			maxId = db.getLong("select max(" + Station.ID.columnName() + ") from " + //
					GeoFrameGeoTable.HYDRO_METEO_STATION.tableName() + " WHERE " + //
					Station.TYPE.columnName() + " = '" + StationSchema.StationType.METEO + "'").intValue();

			processTemperature();
			processPrecipitation();

		} catch (Exception e) {
			// TODO: handle exception
		}
	}

	public void initDb() {
		try {
			if (db == null) {
				db = EDb.GEOPACKAGE.getSpatialDb();
				db.open(inGpkg);
				maxId = db.getLong("select max(" + Station.ID.columnName() + ") from " + //
						GeoFrameGeoTable.HYDRO_METEO_STATION.tableName() + " WHERE " + //
						Station.TYPE.columnName() + " = '" + StationSchema.StationType.METEO + "'").intValue();
			}

		} catch (Exception e) {
			// TODO: handle exception
		}
	}

	public void processTemperature() throws Exception {

		pm.message("Processing temperature data...");
		int type = 4;
		int typeId = VariableSchema.EnvironmentalVariableType.TEMPERATURE.getId();
		if (doDeleteExistingData && db.hasTable(GeoFrameSimpleTable.BASINDATA.getSchema().getSQLName())) {
			db.executeInsertUpdateDeleteSql("DELETE FROM " + GeoFrameSimpleTable.BASINDATA.tableName() + " WHERE " + //
					BasinDataField.VAR_ID.columnName() + " = " + typeId);
		}
		processKriging(db, maxId, type, typeId, false, validatorFor(VariableSchema.EnvironmentalVariableType.TEMPERATURE));

	}

	public void processPrecipitation() throws Exception {
		pm.message("Processing precipitation data...");
		int type = 2; // TODO is this the same as below?
		int typeId = VariableSchema.EnvironmentalVariableType.PRECIPITATION.getId();
		if (doDeleteExistingData && db.hasTable(GeoFrameSimpleTable.BASINDATA.getSchema().getSQLName())) {
			db.executeInsertUpdateDeleteSql("DELETE FROM " + GeoFrameSimpleTable.BASINDATA.tableName() + " WHERE " + //
					BasinDataField.VAR_ID.columnName() + " = " + typeId);
		}
		processKriging(db, maxId, type, typeId, true, validatorFor(VariableSchema.EnvironmentalVariableType.PRECIPITATION));

	}

	private IKrigingOutputValidator validatorFor(VariableSchema.EnvironmentalVariableType type) {
		return VariableRangeValidators.forVariable(type, VariableSchema.TimeResolution.valueOf(pTimeResolution));
	}

	private void processKriging(ASpatialDb db, int maxId, int type, int typeId, boolean boundToZero,
			IKrigingOutputValidator validator) throws Exception {
		var valueReader = new GeoframeEnvDatabaseIterator();
		valueReader.db = db;
		valueReader.pParameterId = type; // temperature
		valueReader.tStart = pStartTimestamp + ":00";
		valueReader.tEnd = pEndTimestamp + ":00";
		valueReader.doRawData = true;
		valueReader.pMaxId = maxId;
		valueReader.preCacheData();

		var krigingInterpolator = new KrigingAtCentroid();
		krigingInterpolator.inGeoframeDBPath = inGpkg;
		krigingInterpolator.inVariableType = typeId;
		krigingInterpolator.variableReader = valueReader;
		krigingInterpolator.cutoffDivide = 10;
		krigingInterpolator.boundToZero = boundToZero;
		krigingInterpolator.valueChecker = validator;
		krigingInterpolator.init();
		krigingInterpolator.process();
	}

	public static void main(String[] args) throws Exception {
		ErmKriging ek = new ErmKriging();
		ek.inGpkg = "/home/andreisd/Documents/project/uni/ARTICOLO_KRIGING/project_grid/data/meteo_data/basin_km9.gpkg";
		ek.pStartTimestamp = "2008-09-01 01:00";
		ek.pEndTimestamp = "2024-06-01 01:00";
		;
		ek.doDeleteExistingData = false;
		ek.process();
	}

}
