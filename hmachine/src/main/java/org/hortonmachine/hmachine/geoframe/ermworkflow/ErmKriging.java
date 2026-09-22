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

@Description("Prepares raster and topological data for the ERM/GeoFrame water budget model pipeline.")
@Author(name = "Daniele Andreis", contact = "")
@Keywords("ERM, GeoFrame, Kriging")
@Label("GeoFrame")
@Name("ermKriging")
@Status(40)
@License("General Public License Version 3 (GPLv3)")
public class ErmKriging extends HMModel {
	@Description("Input geoframe data geopackage.")
	@UI(HMConstants.FILEIN_UI_HINT_VECTOR)
	@In
	public String inGpkg;

	@Description("Data import start timestamp.")
	@In
	public String pStartTimestamp;

	@Description("Data import end timestamp.")
	@In
	public String pEndTimestamp;

	@Description("Delete existing data.")
	@In
	public boolean doDeleteExistingData = false;

	@Execute
	public void process() throws Exception {
		try (ASpatialDb db = EDb.GEOPACKAGE.getSpatialDb();) {
			db.open(inGpkg);
			int maxId = db.getLong("select max(" + Station.ID.columnName() + ") from " + //
					GeoFrameGeoTable.HYDRO_METEO_STATION.tableName() + " WHERE " + //
					Station.TYPE.columnName() + " = '" + StationSchema.StationType.METEO + "'").intValue();

			pm.message("Processing temperature data...");
			int type = 4;
			int typeId = VariableSchema.EnvironmentalVariableType.TEMPERATURE.getId();
//			if (doDeleteExistingData && db.hasTable(GeoFrameSimpleTable.BASINDATA.getSchema().getSQLName())) {
//				db.executeInsertUpdateDeleteSql(
//						"DELETE FROM " + GeoFrameSimpleTable.BASINDATA.tableName() + " WHERE " + //
//								BasinDataField.VAR_ID.columnName() + " = " + typeId);
//			}
			//processKriging(db, maxId, type, typeId, false);

			pm.message("Processing precipitation data...");
			type = 2; // TODO is this the same as below?
			typeId = VariableSchema.EnvironmentalVariableType.PRECIPITATION.getId();
			if (doDeleteExistingData && db.hasTable(GeoFrameSimpleTable.BASINDATA.getSchema().getSQLName())) {
				db.executeInsertUpdateDeleteSql(
						"DELETE FROM " + GeoFrameSimpleTable.BASINDATA.tableName() + " WHERE " + //
								BasinDataField.VAR_ID.columnName() + " = " + typeId);
			}
			processKriging(db, maxId, type, typeId, true);

		}
	}

	private void processKriging(ASpatialDb db, int maxId, int type, int typeId, boolean boundToZero) throws Exception {
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
		krigingInterpolator.init();
		krigingInterpolator.process();
	}

	public static void main(String[] args) throws Exception {
		ErmKriging ek = new ErmKriging();
		ek.inGpkg = "/home/andreisd/Documents/project/uni/ARTICOLO_KRIGING/project_grid/data/meteo_data/basin_km9.gpkg";
		ek.pStartTimestamp = "2008-09-01 01:00";
		ek.pEndTimestamp ="2024-06-01 01:00";;
		ek.doDeleteExistingData = false;
		ek.process();
	}

}
