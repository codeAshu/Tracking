/*
 * Copyright 2012 JBoss Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package machinebalancing.app;

import org.optaplanner.core.api.solver.Solver;
import org.optaplanner.core.api.solver.SolverFactory;
import machinebalancing.domain.CloudBalance;
import machinebalancing.domain.CloudComputer;
import machinebalancing.domain.CloudProcess;
import machinebalancing.persistence.CloudBalancingGenerator;

public class CloudBalancingHelloWorld {

    public static void main(String[] args) {
        // Build the Solver
        SolverFactory solverFactory = SolverFactory.createFromXmlResource(
                "machinebalancing/solver/cloudBalancingSolverConfig.xml");

        Solver solver = solverFactory.buildSolver();

        CloudBalance unsolvedCloudBalance = new WorkerGenerator().createCloudBalance();

        // Load a problem with 400 computers and 1200 processes
       // CloudBalance unsolvedCloudBalance = new CloudBalancingGenerator().createCloudBalance(5, 12);

        // Solve the problem
        solver.solve(unsolvedCloudBalance);
        CloudBalance solvedCloudBalance = (CloudBalance) solver.getBestSolution();

        // Display the result
        System.out.println("\nSolved cloudBalance with 2 computers and 3 processes:\n"
                + toDisplayString(solvedCloudBalance));
    }

    public static String toDisplayString(CloudBalance cloudBalance) {
        StringBuilder displayString = new StringBuilder();
        for (CloudProcess process : cloudBalance.getProcessList()) {
            CloudComputer computer = process.getComputer();
            displayString.append("  ").append(process.getLabel()).append(" -> ")
                    .append(computer == null ? null : computer.getLabel()).append("\n");
        }
        return displayString.toString();
    }

}
