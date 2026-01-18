package org.hortonmachine.geoframe;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.event.KeyEvent;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
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

public class TestCalibration extends HMModel {

	public TestCalibration() throws Exception {

	    //String[] scales = {"15", "12", "9", "6", "3"};
		String[] scales = {"6"};
	    int nRunsPerScale = 10;

	    Path outCsv = Path.of("/home/andreisd/Documents/project/uni/NON_SCALE/pso_bestparams_allscales_befana2.csv");

	    int globalId = 41;

	    for (String scale : scales) {
	        String scaleKey = scale + "km";

	        String geoframeGpkg = "/home/andreisd/Documents/project/uni/NON_SCALE/transform_hm/km" + scale
	                + "/basin_km" + scale + ".gpkg";

	        String envDataPath = "/home/andreisd/Documents/project/uni/NON_SCALE/transform_hm/km" + scale
	                + "/data_km" + scale + ".sql";

	        for (int run = 1; run <= nRunsPerScale; run++) {

	            // ========== (A) APRI DB OGNI RUN ==========
	            ASpatialDb db = EDb.GEOPACKAGE.getSpatialDb();
	            db.open(geoframeGpkg);

	            ADb envDb = EDb.SQLITE.getDb();
	            envDb.open(envDataPath);

	            double[] bestParams = null;

	            try {
	                // ---- setup minimo per quella run (uguale al tuo) ----
	                int maxBasinId = db.getLong("select max(basinid) from " + GeoframeUtils.GEOFRAME_BASIN_TABLE).intValue();

	                QueryResult queryResult = db.getTableRecordsMapIn(GeoframeUtils.GEOFRAME_BASIN_TABLE, null, -1, -1, null);
	                double[] basinAreas = new double[maxBasinId + 1];
	                int idIndex = queryResult.names.indexOf("basinid");
	                for (int i = 0; i < queryResult.data.size(); i++) {
	                    Object[] row = queryResult.data.get(i);
	                    int basinId = (int) row[idIndex];
	                    Geometry basinGeom = (Geometry) row[queryResult.geometryIndex];
	                    double area = basinGeom.getArea() / 1_000_000.0;
	                    basinAreas[basinId] = area;
	                }

	                TopologyNode rootNode = TopologyUtilities.getRootNodeFromDb(db, "153000");

	                String fromTS = "2015-08-01 01:00:00";
	                String toTS   = "2019-10-31 23:00:00";
	                int timeStepMinutes = 60;
	                int spinUpDays = 180;

	                double[] observedDischarge = getObservedDischarge(envDb, fromTS, toTS);
	                int calibrationThreadCount = 5;
	                CostFunctions costFunction = CostFunctions.KGE;

	                var precipReader = new GeoframeEnvDatabaseIterator(maxBasinId);
	                precipReader.db = envDb;
	                precipReader.pParameterId = 2;
	                precipReader.tStart = fromTS;
	                precipReader.tEnd = toTS;
	                precipReader.preCacheData();

	                var tempReader = new GeoframeEnvDatabaseIterator(maxBasinId);
	                tempReader.db = envDb;
	                tempReader.pParameterId = 4;
	                tempReader.tStart = fromTS;
	                tempReader.tEnd = toTS;
	                tempReader.preCacheData();

	                var etpReader = new GeoframeEnvDatabaseIterator(maxBasinId);
	                etpReader.db = envDb;
	                etpReader.pParameterId = 1;
	                etpReader.tStart = fromTS;
	                etpReader.tEnd = toTS;
	                etpReader.preCacheData();

	                IWaterBudgetSimulationRunner runner = new WaterSimulationRunner();
	                int spinUpTimesteps = (24 * 60 / timeStepMinutes) * spinUpDays;

	                PSConfig psConfig = new PSConfig();
	                psConfig.particlesNum = 20;
	                psConfig.maxIterations = 100;
	                psConfig.c1 = 2.0;
	                psConfig.c2 = 2.0;
	                psConfig.w0 = 0.9;
	                psConfig.decay = 0.9;

	                bestParams = WaterBudgetCalibration.psoCalibration(
	                        psConfig, maxBasinId, basinAreas, rootNode,
	                        timeStepMinutes, observedDischarge, costFunction, calibrationThreadCount,
	                        precipReader, tempReader, etpReader, runner, spinUpTimesteps, pm
	                );

	            } finally {
	                // ========== (B) CHIUDI DB OGNI RUN ==========
	                db.close();
	                envDb.close();
	            }

	            // ========== (C) APRI/CHIUDI CSV OGNI RUN (APPEND) ==========
	            appendOneRow(outCsv, globalId, scaleKey, bestParams);

	            // log progresso super leggibile
	            System.out.println("DONE id=" + globalId + " scale=" + scaleKey + " run=" + run);

	            globalId++;
	        }
	    }
	}

	
	private static final String[] CSV_HEADER = {
		    "id","scale",
		    "alfa_r","alfa_s","meltingTemperature","combinedMeltingFactor","freezingFactor","alfa_l",
		    "kc","p","s_RootZoneMax","g","h","pB_soil","sRunoffMax","c","d","s_GroundWaterMax","e","f",
		    "kge"
		};
	
	
	private static void appendOneRow(Path outCsv, int id, String scaleKey, double[] params) throws IOException {

	    boolean writeHeader = !Files.exists(outCsv) || Files.size(outCsv) == 0;

	    try (BufferedWriter w = Files.newBufferedWriter(
	            outCsv,
	            StandardOpenOption.CREATE,
	            StandardOpenOption.APPEND
	    )) {
	        if (writeHeader) {
	            w.write(String.join(",", CSV_HEADER));
	            w.newLine();
	        }

	        // riga nulla
	        if (params == null) {
	            w.write(id + "," + scaleKey + ",NULL");
	            w.newLine();
	            return;
	        }

	        // Atteso: 19 valori (18 params + kge) = CSV_HEADER.length - 2
	        int expected = CSV_HEADER.length - 2;

	        if (params.length != expected) {
	            // Scrivo comunque una riga diagnosticabile senza rompere tutto
	            w.write(id + "," + scaleKey + ",WRONG_LEN_" + params.length + "_EXPECTED_" + expected);
	            w.newLine();
	            return;
	        }

	        StringBuilder row = new StringBuilder();
	        row.append(id).append(",").append(scaleKey);
	        for (double v : params) {
	            row.append(",").append(String.format(Locale.US, "%.10f", v));
	        }
	        w.write(row.toString());
	        w.newLine();
	    }
	}

	private void runSimulationOnParams(ASpatialDb db, int maxBasinId, double[] basinAreas, TopologyNode rootNode,
			String fromTS, int timeStepMinutes, double[] observedDischarge, GeoframeEnvDatabaseIterator precipReader,
			GeoframeEnvDatabaseIterator tempReader, GeoframeEnvDatabaseIterator etpReader,
			IWaterBudgetSimulationRunner runner, int spinUpTimesteps, double[] params) throws Exception {
		runner.configure(timeStepMinutes, maxBasinId, rootNode, basinAreas, true, true, db, pm);
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
		new TestCalibration();
	}

}
