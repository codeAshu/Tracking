package com.useready.tracking.recommendation

import machinebalancing.app.WorkerGenerator
import machinebalancing.domain.{CloudComputer, CloudProcess, CloudBalance}
import org.optaplanner.core.api.solver.{Solver, SolverFactory}

/**
 * Created by Ashu on 20-03-2015.
 */
object PlannerConnect {

  def main(args: Array[String]) {

    // Build the Solver
    val solverFactory: SolverFactory = SolverFactory.createFromXmlResource("machinebalancing/solver/cloudBalancingSolverConfig.xml")
    val solver: Solver = solverFactory.buildSolver
    val unsolvedCloudBalance: CloudBalance = new WorkerGenerator().createCloudBalance
    // Load a problem with 400 computers and 1200 processes
    // CloudBalance unsolvedCloudBalance = new CloudBalancingGenerator().createCloudBalance(5, 12);
    // Solve the problem
    solver.solve(unsolvedCloudBalance)
    val solvedCloudBalance: CloudBalance = solver.getBestSolution.asInstanceOf[CloudBalance]
    // Display the result
    System.out.println("\nSolved cloudBalance with 2 computers and 3 processes:\n" + toDisplayString(solvedCloudBalance))
  }

  def toDisplayString(cloudBalance: CloudBalance) {
    /*
      StringBuilder displayString = new StringBuilder ();
    for (CloudProcess process: cloudBalance.getProcessList () ) {
    CloudComputer computer = process.getComputer ();
    displayString.append ("  ").append (process.getLabel () ).append (" -> ")
    .append (computer == null ? null: computer.getLabel () ).append ("\n");
    }
    return displayString.toString ();
    }
    */
  }
}