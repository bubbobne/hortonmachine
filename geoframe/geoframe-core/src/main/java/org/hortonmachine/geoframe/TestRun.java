package org.hortonmachine.geoframe;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.event.KeyEvent;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import javax.swing.JComponent;
import javax.swing.JDialog;
import javax.swing.KeyStroke;

import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.hortonmachine.dbs.compat.ADb;
import org.hortonmachine.dbs.compat.ASpatialDb;
import org.hortonmachine.dbs.compat.EDb;
import org.hortonmachine.dbs.compat.objects.QueryResult;
import org.hortonmachine.dbs.utils.SqlName;
import org.hortonmachine.gears.io.rasterreader.OmsRasterReader;
import org.hortonmachine.gears.libs.modules.HMConstants;
import org.hortonmachine.gears.libs.modules.HMModel;
import org.hortonmachine.gears.libs.modules.HMRaster;
import org.hortonmachine.gears.libs.monitor.DummyProgressMonitor;
import org.hortonmachine.gears.libs.monitor.IHMProgressMonitor;
import org.hortonmachine.gears.modules.r.cutout.OmsCutOut;
import org.hortonmachine.gears.modules.r.rasteronvectorresizer.OmsRasterResizer;
import org.hortonmachine.gears.modules.r.summary.OmsRasterSummary;
import org.hortonmachine.gears.utils.DynamicDoubleArray;
import org.hortonmachine.gears.utils.RegionMap;
import org.hortonmachine.gears.utils.chart.TimeSeries;
import org.hortonmachine.gears.utils.colors.EColorTables;
import org.hortonmachine.gears.utils.colors.RasterStyleUtilities;
import org.hortonmachine.gears.utils.features.FeatureUtilities;
import org.hortonmachine.gears.utils.files.FileUtilities;
import org.hortonmachine.gears.utils.geometry.GeometryUtilities;
import org.hortonmachine.gears.utils.optimizers.particleswarm.PSConfig;
import org.hortonmachine.gears.utils.optimizers.sceua.CostFunctions;
import org.hortonmachine.gears.utils.optimizers.sceua.ParameterBounds;
import org.hortonmachine.gears.utils.optimizers.sceua.SceUaConfig;
import org.hortonmachine.gears.utils.optimizers.sceua.SceUaOptimizer;
import org.hortonmachine.gears.utils.optimizers.sceua.SceUaResult;
import org.hortonmachine.geoframe.calibration.WaterBudgetCalibration;
import org.hortonmachine.geoframe.calibration.WaterBudgetParameters;
import org.hortonmachine.geoframe.core.TopologyNode;
import org.hortonmachine.geoframe.core.WaterBudgetSimulation;
import org.hortonmachine.geoframe.core.parameters.RainSnowSeparationParameters;
import org.hortonmachine.geoframe.core.parameters.SnowMeltingParameters;
import org.hortonmachine.geoframe.core.parameters.WaterBudgetCanopyParameters;
import org.hortonmachine.geoframe.core.parameters.WaterBudgetGroundParameters;
import org.hortonmachine.geoframe.core.parameters.WaterBudgetRootzoneParameters;
import org.hortonmachine.geoframe.core.parameters.WaterBudgetRunoffParameters;
import org.hortonmachine.geoframe.io.GeoframeEnvDatabaseIterator;
import org.hortonmachine.geoframe.io.GeoframeWaterBudgetSimulationWriter;
import org.hortonmachine.geoframe.utils.IWaterBudgetSimulationRunner;
import org.hortonmachine.geoframe.utils.TopologyUtilities;
import org.hortonmachine.geoframe.utils.WaterSimulationRunner;
import org.hortonmachine.hmachine.modules.demmanipulation.pitfiller.OmsPitfiller;
import org.hortonmachine.hmachine.modules.demmanipulation.wateroutlet.OmsExtractBasin;
import org.hortonmachine.hmachine.modules.geomorphology.draindir.OmsDrainDir;
import org.hortonmachine.hmachine.modules.geomorphology.flow.OmsFlowDirections;
import org.hortonmachine.hmachine.modules.hydrogeomorphology.skyview.OmsSkyview;
import org.hortonmachine.hmachine.modules.network.extractnetwork.OmsExtractNetwork;
import org.hortonmachine.hmachine.modules.network.netnumbering.OmsGeoframeInputsBuilder;
import org.hortonmachine.hmachine.modules.network.netnumbering.OmsNetNumbering;
import org.hortonmachine.hmachine.utils.GeoframeUtils;
import org.jfree.chart.ChartPanel;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Polygon;

import com.google.common.util.concurrent.AtomicDouble;

public class TestRun extends HMModel {

	public TestRun() throws Exception {

		String geoframeGpkg = "/home/andreisd/Documents/project/uni/NON_SCALE/transform_hm/km3/basin_km3.gpkg";

		String envDataPath = "/home/andreisd/Documents/project/uni/NON_SCALE/transform_hm/km3/data_km3.sql";

//		if (!toDo(geoframeGpkg)) {
//			new File(geoframeGpkg).delete();
//		}
		ASpatialDb db = EDb.GEOPACKAGE.getSpatialDb();
		db.open(geoframeGpkg);

		ADb envDb = EDb.SQLITE.getDb();
		envDb.open(envDataPath);

		try {

			// TODO here a potential evapotrans comes in

			// get the max basin id from the db
			int maxBasinId = db.getLong("select max(basinid) from " + GeoframeUtils.GEOFRAME_BASIN_TABLE).intValue();

			// get the basins from the db and their areas
			QueryResult queryResult = db.getTableRecordsMapIn(GeoframeUtils.GEOFRAME_BASIN_TABLE, null, -1, -1, null);
			double[] basinAreas = new double[maxBasinId + 1];
			int idIndex = queryResult.names.indexOf("basinid");
			for (int i = 0; i < queryResult.data.size(); i++) {
				Object[] row = queryResult.data.get(i);
				int basinId = (int) row[idIndex];
				Geometry basinGeom = (Geometry) row[queryResult.geometryIndex];
				double area = basinGeom.getArea() / 1_000_000.0; // in km2
				basinAreas[basinId] = area;
			}

			// get the topology from the db
			TopologyNode rootNode = TopologyUtilities.getRootNodeFromDb(db, "153000");

			//////////////////////////////////////////////////
			/// PARAMETERS
			//////////////////////////////////////////////////
			String fromTS = "2015-08-01 01:00:00";
			String toTS = "2023-12-31 23:00:00";
			var timeStepMinutes = 60; // time step in minutes
			int spinUpDays = 520;
			double[] observedDischarge = getObservedDischarge(envDb, fromTS, toTS);

			var precipReader = new GeoframeEnvDatabaseIterator(maxBasinId);
			precipReader.db = envDb;
			precipReader.pParameterId = 2; // precip
			precipReader.tStart = fromTS;
			precipReader.tEnd = toTS;

			var tempReader = new GeoframeEnvDatabaseIterator(maxBasinId);
			tempReader.db = envDb;
			tempReader.pParameterId = 4; // temperature
			tempReader.tStart = fromTS;
			tempReader.tEnd = toTS;

			var etpReader = new GeoframeEnvDatabaseIterator(maxBasinId);
			etpReader.db = envDb;
			etpReader.pParameterId = 1; // etp
			etpReader.tStart = fromTS;
			etpReader.tEnd = toTS;

			IWaterBudgetSimulationRunner runner = new WaterSimulationRunner();
			int spinUpTimesteps = (24 * 60 / timeStepMinutes) * spinUpDays;

			// best for km15
			//double[] params = {1.0000000000,1.0000000000,0.0000000000,1.2208799735,0.0029708824,0.0450938494,0.6000000000,1.0000000000,343.2155141824,0.0005176691,1.0000000000,0.5012981187,112.6791257729,0.1000037657,1.0000000000,219.8412995250,0.0018663978,1.0000000000};
		
			// best for km12
			 //double[] params = {1.0000000000,1.0000000000,0.0000000000,1.2007258533,0.5497738940,0.1235281152,0.6000000000,1.0000000000,172.3268657857,0.0019165033,1.0000000000,0.5364646072,30.3415507158,0.1000118197,1.0000000000,1458.4739890743,0.0000762617,1.0000000000};

			// best for km9
			 //double[] params = {1.0000000000,1.0000000000,0.0000000000,1.6104233887,0.9767780168,0.2990868935,0.6000000000,1.0000000000,102.9738570288,0.0051373740,1.0000000000,0.5215370248,56.3198292830,0.1003605335,1.0000000000,2499.9923867576,0.0000608434,1.0000000000};

			// best for km6
			 //double[] params = {1.0000000000,1.0000000000,0.0000000000,1.4242516051,0.5592772943,0.1475550062,0.6000000000,1.0000000000,194.6222229412,0.0015901415,1.0000000000,0.6949559192,119.9859420040,0.1000002746,1.0000000000,2499.9978217478,0.0000669369,1.0000000000};

			// best for km3
			double[] params =  {1.0000000000,1.0000000000,0.0000000000,2.2407266384,0.3644761049,0.1421526326,0.6000000000,1.0000000000,325.2391698296,0.0010786613,1.0000000000,1.1409882949,60.6108790792,0.1021049142,1.0000000000,1318.1485190036,0.0000500655,1.0000000000};

			runSimulationOnParams(db, maxBasinId, basinAreas, rootNode, fromTS, timeStepMinutes, observedDischarge,
					precipReader, tempReader, etpReader, runner, spinUpTimesteps, params);

		} finally {
			db.close();
			envDb.close();
		}
	}

	private void runSimulationOnParams(ASpatialDb db, int maxBasinId, double[] basinAreas, TopologyNode rootNode,
			String fromTS, int timeStepMinutes, double[] observedDischarge, GeoframeEnvDatabaseIterator precipReader,
			GeoframeEnvDatabaseIterator tempReader, GeoframeEnvDatabaseIterator etpReader,
			IWaterBudgetSimulationRunner runner, int spinUpTimesteps, double[] params) throws Exception {
		runner.configure(timeStepMinutes, maxBasinId, rootNode, basinAreas, false, true, db, pm);
		WaterBudgetParameters wbParams = WaterBudgetParameters.fromParameterArray(params);

		// run a single simulation with default parameters
		double[] simQ = runner.run(wbParams, 0.6, // TODO handle LAI properly
				precipReader, tempReader, etpReader, null);

		double cost = CostFunctions.KGE.evaluateCost(observedDischarge, simQ, spinUpTimesteps,
				HMConstants.doubleNovalue);
		String title = "Simulated vs Observed Discharge ( cost: " + cost + " )";
		chartResult(title, simQ, observedDischarge, timeStepMinutes, fromTS);
	}

	private void chartResult(String title, double[] simQ, double[] observedDischarge, int timeStepMinutes,
			String fromTS) {
		String xLabel = "time";
		String yLabel = "Q [m3]";
		int width = 1600;
		int height = 1000;

		List<String> series = new ArrayList<>();
		series.add("Simulated Discharge");
		series.add("Observed Discharge");
		List<Boolean> doLines = new ArrayList<>();
		doLines.add(true);
		doLines.add(true);

		long startTS = GeoframeEnvDatabaseIterator.str2ts(fromTS);

		List<double[]> allValuesList = new ArrayList<>();
		List<long[]> allTimesList = new ArrayList<>();
		// simulated
		double[] simValues = new double[simQ.length];
		double[] obsValues = new double[simQ.length];
		long[] simTimes1 = new long[simQ.length];
		long[] simTimes2 = new long[simQ.length];
		for (int i = 0; i < simQ.length; i++) {
			simValues[i] = simQ[i];
			simTimes1[i] = startTS + i * timeStepMinutes * 60 * 1000L;

			if (!HMConstants.isNovalue(observedDischarge[i])) {
				obsValues[i] = observedDischarge[i];
			} else {
				obsValues[i] = 0.0;
			}
			simTimes2[i] = startTS + i * timeStepMinutes * 60 * 1000L;
		}

		allValuesList.add(simValues);
		allTimesList.add(simTimes1);

		allValuesList.add(obsValues);
		allTimesList.add(simTimes2);

		TimeSeries timeseriesChart = new TimeSeries(title, series, allTimesList, allValuesList);
		timeseriesChart.setXLabel(xLabel);
		timeseriesChart.setYLabel(yLabel);
		timeseriesChart.setShowLines(doLines);

		timeseriesChart.setColors(new Color[] { Color.BLUE, Color.RED });
//	        if (doShapes != null)
//	            timeseriesChart.setShowShapes(doShapes);

		ChartPanel chartPanel = new ChartPanel(timeseriesChart.getChart(), true);
		Dimension preferredSize = new Dimension(width, height);
		chartPanel.setPreferredSize(preferredSize);

//	        GuiUtilities.openDialogWithPanel(chartPanel, "HM Chart Window", preferredSize, false);
		JDialog f = new JDialog();
		f.add(chartPanel, BorderLayout.CENTER);
		f.setTitle(title);
		f.setModal(false);
		f.pack();
//	        if (dimension != null)
//	            f.setSize(dimension);
		f.setLocationRelativeTo(null); // Center on screen
		f.setVisible(true);
		f.setDefaultCloseOperation(JDialog.DISPOSE_ON_CLOSE);
		f.getRootPane().registerKeyboardAction(e -> {
			f.dispose();
		}, KeyStroke.getKeyStroke(KeyEvent.VK_ESCAPE, 0), JComponent.WHEN_IN_FOCUSED_WINDOW);

	}

	private double[] getObservedDischarge(ADb envDb, String fromTS, String toTS) throws Exception {
		long from = GeoframeEnvDatabaseIterator.str2ts(fromTS);
		long to = GeoframeEnvDatabaseIterator.str2ts(toTS);
		String sql = "select ts, value from observed_discharge_153000 where ts >= " + from + " " + "and ts <= " + to
				+ " order by ts asc";
		QueryResult qr = envDb.getTableRecordsMapFromRawSql(sql, -1);
		DynamicDoubleArray dda = new DynamicDoubleArray(10000, 10000);
		int valueIndex = qr.names.indexOf("value");
		for (Object[] row : qr.data) {
			double value = ((Number) row[valueIndex]).doubleValue();
			dda.addValue(value);
		}
		return dda.getTrimmedInternalArray();
	}

	private boolean toDo(String filepath) {
		return new File(filepath).exists() == false;
	}

	public static void makeQgisStyleForRaster(String tableName, String rasterPath, int labelDecimals) throws Exception {
		OmsRasterSummary s = new OmsRasterSummary();
		s.pm = new DummyProgressMonitor();
		s.inRaster = OmsRasterReader.readRaster(rasterPath);
		s.process();
		double min = s.outMin;
		double max = s.outMax;

		String style = RasterStyleUtilities.createQGISRasterStyle(tableName, min, max, null, labelDecimals);
		File styleFile = FileUtilities.substituteExtention(new File(rasterPath), "qml");
		FileUtilities.writeFile(style, styleFile);
	}

	public static void main(String[] args) throws Exception {
		new TestRun();
	}

}
