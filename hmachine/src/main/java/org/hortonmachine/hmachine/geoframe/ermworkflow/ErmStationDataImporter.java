package org.hortonmachine.hmachine.geoframe.ermworkflow;

import java.nio.file.Files;
import java.nio.file.Path;

import org.hortonmachine.gears.libs.modules.HMConstants;
import org.hortonmachine.gears.libs.modules.HMModel;
import org.hortonmachine.hmachine.geoframe.io.database.importer.GeoframeRawDataImporter;
import org.hortonmachine.hmachine.geoframe.io.database.tables.implementation.StationSchema.StationType;
import org.hortonmachine.hmachine.geoframe.io.database.tables.implementation.VariableSchema.EnvironmentalVariableType;
import org.hortonmachine.hmachine.geoframe.io.database.tables.implementation.VariableSchema.TimeResolution;

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

@Description("Importer of raw meteo and stream gauge data into the GeoFrame database.")
@Author(name = "Daniele Andreis", contact = "")
@Keywords("ERM, GeoFrame, DEM, basin, network, data preparation")
@Label("GeoFrame")
@Name("ermRawDataImporter")
@Status(40)
@License("General Public License Version 3 (GPLv3)")
public class ErmStationDataImporter extends HMModel {

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

	@Description("Data time resolution. currently supported: HOURLY, DAILY.")
	@In
	public String pTimeResolution = "HOURLY";

	@Description("Meteo stations layer.")
	@UI(HMConstants.FILEIN_UI_HINT_VECTOR)
	@In
	public String inMeteoStations;

	@Description("Station id field in meteo csv files.")
	@In
	public String pMeteoIdField = "ID";
	
	@Description("Station id field in meteo csv files.")
	@In
	public String pActualIdField = "ID";
	@Description("Station id field in meteo csv files.")
	@In
	public String pProvider = "ID";

	@Description("Temperatures csv file.")
	@UI(HMConstants.FILEIN_UI_HINT_CSV)
	@In
	public String inTemperaturesCsv;

	@Description("Precipitation csv file.")
	@UI(HMConstants.FILEIN_UI_HINT_CSV)
	@In
	public String inPrecipitationCsv;

	@Description("Stream Gauges layer.")
	@UI(HMConstants.FILEIN_UI_HINT_VECTOR)
	@In
	public String inStreamGauges;

	@Description("Streamgauges id field in csv files.")
	@In
	public String pStreamGaugesIdField = "ID";

	@Description("Stream Gauges data csv file.")
	@UI(HMConstants.FILEIN_UI_HINT_CSV)
	@In
	public String inStreamGaugesCsv;

	@Execute
	public void process() throws Exception {
		checkNull(pStartTimestamp, pEndTimestamp);
		checkFileExists(inGpkg);
		var gfImporter = new GeoframeRawDataImporter();
		gfImporter.inGeoframeDBPath = inGpkg;
		gfImporter.inStartDate = pStartTimestamp;
		gfImporter.inEndDate = pEndTimestamp;
		gfImporter.inElevationField = "z_dem";
		gfImporter.inActualFieldId = "ACT_ID";
		gfImporter.inProviderFieldId = "PROVIDER";


		gfImporter.inIdField = pMeteoIdField;
		gfImporter.timeResolution = TimeResolution.valueOf(pTimeResolution);
		gfImporter.doOverWrite = true;

		// import TEMPERATURE
		if (inTemperaturesCsv != null && Files.exists(Path.of(inTemperaturesCsv))) {
			gfImporter.inMeasurementsPointFilePath = inMeteoStations;
			gfImporter.inMeasurementDataFilePath = inTemperaturesCsv;
			gfImporter.stationType = StationType.METEO;
			gfImporter.inVariableType = EnvironmentalVariableType.TEMPERATURE.getId();
			gfImporter.process();
		}

		// import the precipitation
		gfImporter.doOverWrite = false;
		if (inPrecipitationCsv != null && Files.exists(Path.of(inPrecipitationCsv))) {
			gfImporter.inMeasurementsPointFilePath = null;
			gfImporter.inMeasurementDataFilePath = inPrecipitationCsv;
			gfImporter.stationType = StationType.METEO;
			gfImporter.inVariableType = EnvironmentalVariableType.PRECIPITATION.getId();
			gfImporter.process();
		}

		if (inStreamGaugesCsv != null && Files.exists(Path.of(inStreamGaugesCsv))) {
			gfImporter.doOverWrite = false;

			gfImporter.inMeasurementDataFilePath = inStreamGaugesCsv;
			gfImporter.inMeasurementsPointFilePath = inStreamGauges;
			gfImporter.inIdField = pStreamGaugesIdField;	
			gfImporter.inElevationField = null;
			gfImporter.stationType = StationType.STREAM_GAUGE;
			gfImporter.inVariableType = EnvironmentalVariableType.DISCHARGE.getId();
			gfImporter.process();
		}

	}

	public static void main(String[] args) throws Exception {
		String workspace = "/home/andreisd/Desktop/geoframe_sito_API/data/";
		ErmStationDataImporter ei = new ErmStationDataImporter();
		ei.inGpkg = workspace + "/basin_km9.gpkg";
		ei.pStartTimestamp = "1990-01-01 01:00";
		ei.pEndTimestamp = "2024-07-17 23:00";
		ei.pTimeResolution = ErmCommonData.TIME_RESOLUTION;
//		ei.inMeteoStations = "/home/andreisd/Documents/project/uni/ARTICOLO_KRIGING/project_grid/data/meteo_data/stations_tot.shp";
//		ei.inTemperaturesCsv = "/home/andreisd/Documents/project/uni/ARTICOLO_KRIGING/project_grid/data/meteo_data/"
//				+ "temperature_kriging_ready.csv";
//		ei.inPrecipitationCsv = "/home/andreisd/Documents/project/uni/ARTICOLO_KRIGING/project_grid/data/meteo_data/"
//				+ "precipitation_kriging_ready.csv";
//
//		ei.process();

		
		ei.pStartTimestamp = "2000-01-01 01:00";
		ei.pEndTimestamp = "2024-12-31 23:00";
		ei.pTimeResolution = ErmCommonData.TIME_RESOLUTION;
		ei.inMeteoStations = null;
		ei.inTemperaturesCsv = null;
		ei.inPrecipitationCsv = null;

		ei.inStreamGauges = "/home/andreisd/Documents/project/uni/ARTICOLO_KRIGING/project_grid/data/Trentino/Noce/idrometri_sgiustinacorto_pochi.shp";
		ei.pStreamGaugesIdField = "idstazione";
		ei.inStreamGaugesCsv = "/home/andreisd/Documents/project/uni/ARTICOLO_KRIGING/simulazioni/GU10-GRIDW/data/Q_vermiglio.csv";

		//ei.process();

		ei.inStreamGauges = null;
		ei.pStreamGaugesIdField = "idstazione";
		ei.inStreamGaugesCsv = "/home/andreisd/Documents/project/uni/ARTICOLO_KRIGING/simulazioni/GU10-GRIDW/data/Q_male.csv";
		ei.process();

	}
}
