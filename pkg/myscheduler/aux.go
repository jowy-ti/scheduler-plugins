package myscheduler

import (
	"fmt"
	"math"
	"strconv"

	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/cache"
	framework "k8s.io/kubernetes/pkg/scheduler/framework"
)

const (
	migInstances        string          = "mig-instances"
	gpuResourceName     v1.ResourceName = "nvidia.com/gpu"
	maxAvailabilityGpu  int             = 100
	gpuMemory           string          = "nvidia.com/gpu.memory"
	gpufp32GFLOPS       string          = "nvidia.com/gpu.fp32.GFLOPS"
	podRequestGpuMemory v1.ResourceName = "customresource.com/gpuMemory"
	podRequestGpufp32   v1.ResourceName = "customresource.com/gpufp32"
	filterPodLabel      string          = "app"
	filterPodLabelValue string          = "fake-pod"
)

// Funciones auxiliares

// onDelete

func onDelete(obj interface{}) {
	pod, ok := obj.(*v1.Pod)

	if !ok {
		unknown, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			return
		}
		pod, ok = unknown.Obj.(*v1.Pod)
		if !ok {
			return
		}
	}
	var podName string = pod.Name
	podsUsage.cleanPodResources(podName)
}

// FilterFunc

func filterFunc(obj interface{}) bool {
	pod, ok := obj.(*v1.Pod)

	if !ok {
		return false
	}
	var podLabels map[string]string = pod.Labels
	return podLabels[filterPodLabel] == filterPodLabelValue
}

// Construccion de la estructura de datos en cache de los nodos
func gpuNodeBuild(nodeInfo *framework.NodeInfo) error {

	var nodeName string = nodeInfo.GetName()
	var nodeLabels map[string]string = nodeInfo.Node().Labels
	var gpuCount int64 = nodeInfo.Allocatable.ScalarResources[gpuResourceName]

	labelMemoryGpu, ok := nodeLabels[gpuMemory]
	if !ok {
		return fmt.Errorf("label %s not found in node %s", gpuMemory, nodeName)
	}

	memoryGpu, err := strconv.ParseInt(labelMemoryGpu, 10, 0)
	if err != nil {
		return fmt.Errorf("error to convert string to int for string %s in node %s", labelMemoryGpu, nodeName)
	}

	labelfp32Gpu, ok := nodeLabels[gpufp32GFLOPS]
	if !ok {
		return fmt.Errorf("label %s not found in node %s", gpufp32GFLOPS, nodeName)
	}

	fp32Gpu, err := strconv.ParseInt(labelfp32Gpu, 10, 0)
	if err != nil {
		return fmt.Errorf("error to convert string to int for string %s in node %s", labelfp32Gpu, nodeName)
	}

	labelInstances, ok := nodeLabels[migInstances]
	if !ok {
		return fmt.Errorf("label %s not found in node %s", migInstances, nodeName)
	}
	numInstances, err := strconv.ParseInt(labelInstances, 10, 0)

	if err != nil {
		return fmt.Errorf("error to convert string to int for string %s in node %s", labelInstances, nodeName)
	}

	var gpus []*gpuSpec = make([]*gpuSpec, gpuCount)

	for i := int64(0); gpuCount > i; i++ {
		gpus[i] = newGpuSpec()
		gpus[i].setGpuSpecGpuOnly(int(memoryGpu), int(fp32Gpu), int(numInstances))
	}

	if numInstances > 0 {
		for i := 0; int(gpuCount) > i; i++ {
			var migGeometry []*migSlice = make([]*migSlice, int(numInstances))
			migGeometry[0] = newMigSlice()
			migGeometry[0].setInfoMigSlice(int(numInstances), int(memoryGpu), int(fp32Gpu))
			gpus[i].setGpuSpecMigOnly(migGeometry)
		}
	}

	nodeGpus.setAllNodesGpus(gpus, nodeName)
	return nil
}

// Computa el total de recursos que solicita un pod
func computePodResourceRequest(pod *v1.Pod) *framework.Resource {
	var result *framework.Resource = &framework.Resource{}

	for _, container := range pod.Spec.Containers {
		result.Add(container.Resources.Requests)
	}

	// take max_resource(sum_pod, any_init_container)
	for _, container := range pod.Spec.InitContainers {
		result.SetMaxResource(container.Resources.Requests)
	}

	// If Overhead is being utilized, add to the total requests for the pod
	if pod.Spec.Overhead != nil {
		result.Add(pod.Spec.Overhead)
	}

	return result
}

// Obtencion del StateData dentro del CycleState
func getPreFilterState(cycleState *framework.CycleState) (*PreFilterState, error) {
	stateData, err := cycleState.Read(preFilterStateKey)

	if err != nil {
		// preFilterState doesn't exist, likely PreFilter wasn't invoked.
		return nil, fmt.Errorf("error reading %q from cycleState: %w", preFilterStateKey, err)
	}

	preFilterState, ok := stateData.(*PreFilterState)
	if !ok {
		return nil, fmt.Errorf("%+v  convert to NodeResourcesFit.preFilterState error", stateData)
	}
	return preFilterState, nil
}

// Verifica si el nodo tiene recursos suficientes para el pod
func enoughNodeResources(nodeName string, availableNodeCpu int, availableNodeMem int, podRequests *framework.Resource) *framework.Status {

	if availableNodeCpu < int(podRequests.MilliCPU) {
		return framework.NewStatus(framework.Unschedulable, "CPU Insuficiente")
	}
	if availableNodeMem < int(podRequests.Memory) {
		return framework.NewStatus(framework.Unschedulable, "Memoria insuficiente")
	}

	totalGpus, err := nodeGpus.getLength(nodeName)

	if err != nil {
		return framework.NewStatus(framework.Unschedulable, err.Error())
	}

	for i := 0; totalGpus > i; i++ {
		gpu, err := nodeGpus.getGeneralGpuResources(nodeName, i)
		if err != nil {
			return framework.NewStatus(framework.Unschedulable, err.Error())
		}

		if gpu.migLength == 0 {
			var gpuAvailable float64 = float64(gpu.available) / float64(maxAvailabilityGpu)
			var gpuMemAvailable int = int(gpuAvailable * float64(gpu.mem))
			var gpuFp32Available int = int(gpuAvailable * float64(gpu.fp32))

			if gpuMemAvailable > int(podRequests.ScalarResources[podRequestGpuMemory]) && gpuFp32Available > int(podRequests.ScalarResources[podRequestGpufp32]) {
				return framework.NewStatus(framework.Success)
			}
		} else {
			for j := 0; gpu.migLength > j; j++ {
				migPartition := gpu.migSlices[j]

				var migAvailable float64 = float64(migPartition.available) / float64(maxAvailabilityGpu)
				var migMemAvailable int = int(migAvailable * float64(migPartition.mem))
				var migFp32Available int = int(migAvailable * float64(migPartition.fp32))

				if migMemAvailable > int(podRequests.ScalarResources[podRequestGpuMemory]) && migFp32Available > int(podRequests.ScalarResources[podRequestGpufp32]) {
					return framework.NewStatus(framework.Success)
				}

				j += migPartition.size - 1
			}
		}
	}

	return framework.NewStatus(framework.Unschedulable, "Recursos insuficientes")
}

func nodeGpusUsage(nodeName string) (int64, *framework.Status) {

	var totalGpuAvailable int = 0
	numGpus, err := nodeGpus.getLength(nodeName)

	if err != nil {
		return 0, framework.NewStatus(framework.Error, err.Error())
	}

	for i := 0; numGpus > i; i++ {
		gpu, err := nodeGpus.getGeneralGpuResources(nodeName, i)

		if err != nil {
			return 0, framework.NewStatus(framework.Error, err.Error())
		}

		if gpu.migLength == 0 {
			totalGpuAvailable += gpu.available
		} else {
			var totalMigAvailable int = 0
			var migPartitions int = 0

			for j := 0; gpu.migLength > j; j++ {
				totalMigAvailable += gpu.migSlices[j].available

				migPartitions++
				j += gpu.migSlices[j].size - 1
			}

			totalGpuAvailable += totalMigAvailable / migPartitions
		}
	}

	var res int64 = int64(maxAvailabilityGpu - (totalGpuAvailable / numGpus))

	return res, framework.NewStatus(framework.Success)
}

func gpuReservation(podName string, nodeName string, podRequests *framework.Resource) error {

	length, err := nodeGpus.getLength(nodeName)

	if err != nil {
		return err
	}

	var leastAvailableValue int = maxAvailabilityGpu + 1
	var leastAvailableGpuPosition int = -1
	var gpuAssgined int = 0
	var fp32Req float64 = float64(podRequests.ScalarResources[podRequestGpufp32])
	var memReq float64 = float64(podRequests.ScalarResources[podRequestGpuMemory])

	for gpuPosition := 0; length > gpuPosition; gpuPosition++ {
		gpu, err := nodeGpus.getGeneralGpuResources(nodeName, gpuPosition)

		if err != nil {
			return err
		}

		var fp32Ratio float64 = fp32Req / float64(gpu.fp32)
		var memRatio float64 = memReq / float64(gpu.mem)
		var gpuReqRatio float64 = math.Max(fp32Ratio, memRatio)
		var gpuReq int = int(gpuReqRatio * float64(maxAvailabilityGpu))

		if gpu.available < leastAvailableValue && gpu.available >= gpuReq {
			leastAvailableValue = gpu.available
			leastAvailableGpuPosition = gpuPosition
			gpuAssgined = gpuReq
		}
	}

	if leastAvailableGpuPosition < 0 || leastAvailableGpuPosition >= length || gpuAssgined <= 0 {
		return fmt.Errorf("no se ha encontrado adecuadamente una gpu para realizar la reserva")
	}

	err = podsUsage.setPodResourcesGpuOnly(podName, nodeName, leastAvailableGpuPosition, gpuAssgined)

	if err != nil {
		return err
	}

	return nil
}

func migReservation(podName string, nodeName string, podRequests *framework.Resource) error {
	return nil
}
