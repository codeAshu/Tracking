package machinebalancing.app;

import machinebalancing.domain.CloudBalance;
import machinebalancing.domain.CloudComputer;
import machinebalancing.domain.CloudProcess;

import java.util.ArrayList;
import java.util.List;

public class WorkerGenerator {
    public CloudBalance createCloudBalance() {

        CloudBalance c = new CloudBalance();

        //create 2 workers
        List<CloudComputer> computerList = new ArrayList<CloudComputer>(2);

        CloudComputer w1 = new CloudComputer();
        w1.setCpuPower(4);
        w1.setMemory(8);
        w1.setNetworkBandwidth(2);
        w1.setCost(140);
        w1.setId(1l);

        CloudComputer w2 = new CloudComputer();
        w2.setCpuPower(4);
        w2.setMemory(16);
        w2.setNetworkBandwidth(2);
        w2.setCost(300);
        w2.setId(2l);

        computerList.add(w1);computerList.add(w2);

        //create process
        List<CloudProcess> processList = new ArrayList<CloudProcess>(5);
        CloudProcess  op1 = new CloudProcess();
        op1.setRequiredCpuPower(3);
        op1.setRequiredMemory(2);
        op1.setRequiredNetworkBandwidth(1);
        op1.setId(1l);

        CloudProcess  op2 = new CloudProcess();
        op2.setRequiredCpuPower(3);
        op2.setRequiredMemory(2);
        op2.setRequiredNetworkBandwidth(1);
        op2.setId(2l);

        CloudProcess  op3 = new CloudProcess();
        op3.setRequiredCpuPower(3);
        op3.setRequiredMemory(2);
        op3.setRequiredNetworkBandwidth(1);
        op3.setId(3l);
        processList.add(op1);processList.add(op2);processList.add(op3);
        c.setComputerList(computerList);
        c.setProcessList(processList);
        c.setId(0l);
        return c;
    }
}
