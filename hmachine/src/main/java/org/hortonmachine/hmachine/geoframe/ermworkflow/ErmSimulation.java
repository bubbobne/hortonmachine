package org.hortonmachine.hmachine.geoframe.ermworkflow;

import org.hortonmachine.gears.libs.modules.HMConstants;
import org.hortonmachine.gears.utils.optimizers.CostFunctions;
import org.hortonmachine.hmachine.geoframe.calibration.WaterBudgetParameters;
import org.hortonmachine.hmachine.geoframe.utils.IWaterBudgetSimulationRunner;

import oms3.annotations.Author;
import oms3.annotations.Description;
import oms3.annotations.Execute;
import oms3.annotations.In;
import oms3.annotations.Keywords;
import oms3.annotations.Label;
import oms3.annotations.License;
import oms3.annotations.Name;
import oms3.annotations.Status;

@Description("Runs a single ERM/GeoFrame water budget simulation with a fixed parameter set.")
@Author(name = "Andrea Antonello", contact = "https://g-ant.eu")
@Keywords("ERM, GeoFrame, simulation, water budget")
@Label("GeoFrame")
@Name("ermSimulation")
@Status(40)
@License("General Public License Version 3 (GPLv3)")
public class ErmSimulation extends ErmBase {

    @Description("Model parameter array (18 values).")
    @In
    public double[] inParams;

    @Execute
    public void process() throws Exception {
        setup();
        try {
            runner.configure(pTimeStepMinutes, maxBasinId, rootNode, basinAreas,
                    true, true, doWriteState, db, pm);

            WaterBudgetParameters wbParams = WaterBudgetParameters.fromParameterArray(inParams);
            double[] simQ = runner.run(wbParams, 0.6, precipReader, tempReader, etpReader, null);

            double cost = Math.round(
                    -CostFunctions.KGE.evaluateCost(observedDischarge, simQ, spinUpTimesteps,
                            HMConstants.doubleNovalue) * 100.0) / 100.0;
            pm.message("KGE = " + cost);

            IWaterBudgetSimulationRunner.quickChartResult(
                    "Simulated vs Observed Discharge ( KGE: " + cost + " )",
                    simQ, observedDischarge, pTimeStepMinutes, inFromTimestamp, spinUpTimesteps);
        } finally {
            teardown();
        }
    }


    public static void main( String[] args ) throws Exception {
        ErmSimulation sim = new ErmSimulation();
        sim.inGeopackagePath = "/home/andreisd/Documents/project/uni/ARTICOLO_KRIGING/project_grid/data/meteo_data/basin_km9.gpkg";
        sim.inFromTimestamp = "2000-01-01 01:00" + ":00";
    	sim.inToTimestamp ="2024-01-01 01:00" + ":00";
    	sim.pTimeStepMinutes = 60;
    	sim.pSpinUpDays = 365;
    	sim.doWriteState = true;
        sim.inParams = new double[]{
        		1, 1, 0, 2.2, 0.44, 0.3361495, 0.55497,0.1848878 , 280.0676272, 2.3063311963866795E-4,1.022862684020969, 0.6283329246350391, 76.92825463263878, 0.0013370514467183625, 
        		1.7501590895191832, 1515.4296934634467, 7.824869535610901E-4, 1.0
            }; // 0.8369
        
        sim.process();
    }
}
