package myscheduler

import (
	"fmt"
	"math"
	"strconv"

	v1 "k8s.io/api/core/v1"
	framework "k8s.io/kubernetes/pkg/scheduler/framework"
)

const (
	migSlices                string          = "mig-instances"
	gpuResourceName          v1.ResourceName = "nvidia.com/gpu"
	maxAvailabilityGpu       int             = 1000
	gpuMemory                string          = "nvidia.com/gpu.memory"
	gpufp32GFLOPS            string          = "nvidia.com/gpu.fp32.GFLOPS"
	podRequestGpuMemory      v1.ResourceName = "customresource.com/gpuMemory"
	podRequestGpufp32        v1.ResourceName = "customresource.com/gpufp32"
	filterPodLabel           string          = "app"
	filterPodLabelValue      string          = "fake-pod"
	invalidInstanceSize      int             = -1
	MIG_7_ROWS               int             = 19
	MIG_7_COLUMNS            int             = 7
	GPU_POS_ANNOTATION       string          = "gpuPos"
	MIG_SIZE_ANNOTATION      string          = "migSize"
	NO_MIG_INSTANCE_ASSIGNED int             = -1
)

var MIG_PROFILES_7_INSTANCES [MIG_7_ROWS][MIG_7_COLUMNS]int = [MIG_7_ROWS][MIG_7_COLUMNS]int{
	// Cfg | S0 | S1 | S2 | S3 | S4 | S5 | S6 |  Descripción Visual
	// -------------------------------------------------------------------
	/* 1  */ {7, 0, 0, 0, 0, 0, 0}, // 1x 7g (Toda la fila verde)
	/* 2  */ {4, 0, 0, 0, 3, 0, 0}, // 4g (Naranja) + 3g (Amarillo)
	/* 3  */ {4, 0, 0, 0, 2, 0, 1}, // 4g (Naranja) + 2g (Azul) + 1g
	/* 4  */ {4, 0, 0, 0, 1, 1, 1}, // 4g (Naranja) + 3x 1g
	/* 5  */ {3, 0, 0, 3, 0, 0, -1}, // 3g (Amarillo) + 3g (Amarillo) + GRIS
	/* 6  */ {3, 0, 0, 2, 0, 1, -1}, // 3g (Amarillo) + 2g (Azul) + 1g + GRIS
	/* 7  */ {3, 0, 0, 1, 1, 1, -1}, // 3g (Amarillo) + 3x 1g + GRIS
	/* 8  */ {2, 0, 2, 0, 3, 0, 0}, // 2g + 2g + 3g (Amarillo al final)
	/* 9  */ {2, 0, 1, 1, 3, 0, 0}, // 2g + 1g + 1g + 3g (Amarillo al final)
	/* 10 */ {1, 1, 2, 0, 3, 0, 0}, // 1g + 1g + 2g + 3g (Amarillo al final)
	/* 11 */ {1, 1, 1, 1, 3, 0, 0}, // 4x 1g + 3g (Amarillo al final)
	/* 12 */ {2, 0, 2, 0, 2, 0, 1}, // 2g + 2g + 2g + 1g
	/* 13 */ {2, 0, 1, 1, 2, 0, 1}, // 2g + 1g + 1g + 2g + 1g
	/* 14 */ {1, 1, 2, 0, 2, 0, 1}, // 1g + 1g + 2g + 2g + 1g
	/* 15 */ {2, 0, 1, 1, 1, 1, 1}, // 2g + 5x 1g
	/* 16 */ {1, 1, 2, 0, 1, 1, 1}, // 1g + 1g + 2g + 3x 1g
	/* 17 */ {1, 1, 1, 1, 2, 0, 1}, // 4x 1g + 2g + 1g
	/* 18 */ {1, 1, 1, 1, 1, 2, 0}, // 5x 1g + 2g (Azul al final ocupando S5 y S6)
	/* 19 */ {1, 1, 1, 1, 1, 1, 1}, // 7x 1g (Todo rosa)
}

var MIG_7_MEMORY_FRACTION = map[int]float64{
	1: 1.0 / 8.0,
	2: 2.0 / 8.0,
	3: 4.0 / 8.0,
	4: 4.0 / 8.0,
	7: 1.0,
}

var MIG_7_COMPUTE_FRACTION = map[int]float64{
	1: 1.0 / 7.0,
	2: 2.0 / 7.0,
	3: 3.0 / 7.0,
	4: 4.0 / 7.0,
	7: 1.0,
}

var MIG_PROFILES_7_RESOURCES_UNUSED = func() [MIG_7_ROWS]int {
	var resourcesUnused [MIG_7_ROWS]int

	for i := 0; MIG_7_ROWS > i; i++ {
		var resourcesUsed float64 = 0
		var migSize int

		for j := 0; MIG_7_COLUMNS > j; j += absInt(migSize) {
			migSize = MIG_PROFILES_7_INSTANCES[i][j]

			if migSize == invalidInstanceSize {
				continue
			}

			resourcesUsed += MIG_7_MEMORY_FRACTION[migSize] + MIG_7_COMPUTE_FRACTION[migSize]
		}
		var unused float64 = float64(maxAvailabilityGpu) - ((resourcesUsed / 2.0) * float64(maxAvailabilityGpu))
		resourcesUnused[i] = int(unused)
	}
	return resourcesUnused
}()

// Funciones auxiliares

func absInt(n int) int {
	if n < 0 {
		return -n
	}
	return n
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

func extractPodTimes(podAnnotations map[string]string, podName string) (scheduled int64, deletion int64, err error) {

	var scheduledTime int64
	var deletionTime int64

	scheduledTimeAnnotation, ok := podAnnotations[scheduledAnnotation]

	if !ok {
		return 0, 0, fmt.Errorf("no se ha encontrado la anotación %s en el pod %s", scheduledAnnotation, podName)
	}

	scheduledTime, err = strconv.ParseInt(scheduledTimeAnnotation, 10, 0)

	if err != nil {
		return 0, 0, err
	}

	deletionTimeAnnotation, ok := podAnnotations[deletionAnnotation]

	if !ok {
		return 0, 0, fmt.Errorf("no se ha encontrado la anotación %s en el pod %s", deletionAnnotation, podName)
	}

	deletionTime, err = strconv.ParseInt(deletionTimeAnnotation, 10, 0)

	if err != nil {
		return 0, 0, err
	}

	return scheduledTime, deletionTime, nil
}

// Extrae información del nodo para poder construirlo
func extractNodeInfo(nodeLabels map[string]string, nodeName string) (gpuMem int, gpuFp32 int, nInstances int, err error) {

	labelMemoryGpu, ok := nodeLabels[gpuMemory]
	if !ok {
		return 0, 0, 0, fmt.Errorf("label %s not found in node %s", gpuMemory, nodeName)
	}

	memoryGpu, err := strconv.ParseInt(labelMemoryGpu, 10, 0)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("error to convert string to int for string %s in node %s", labelMemoryGpu, nodeName)
	}

	labelfp32Gpu, ok := nodeLabels[gpufp32GFLOPS]
	if !ok {
		return 0, 0, 0, fmt.Errorf("label %s not found in node %s", gpufp32GFLOPS, nodeName)
	}

	fp32Gpu, err := strconv.ParseInt(labelfp32Gpu, 10, 0)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("error to convert string to int for string %s in node %s", labelfp32Gpu, nodeName)
	}

	labelInstances, ok := nodeLabels[migSlices]
	if !ok {
		return 0, 0, 0, fmt.Errorf("label %s not found in node %s", migSlices, nodeName)
	}
	numInstances, err := strconv.ParseInt(labelInstances, 10, 0)

	if err != nil {
		return 0, 0, 0, fmt.Errorf("error to convert string to int for string %s in node %s", labelInstances, nodeName)
	}

	return int(memoryGpu), int(fp32Gpu), int(numInstances), nil
}

// Construccion de la estructura de datos en cache de los nodos
func gpuNodeBuild(nodeName string, memoryGpu int, fp32Gpu int, numInstances int, gpuCount int) error {

	var gpus []*gpuSpec = make([]*gpuSpec, gpuCount)

	for i := 0; gpuCount > i; i++ {
		gpus[i] = newGpuSpec()
		gpus[i].setGpuSpecGpuOnly(int(memoryGpu), int(fp32Gpu), int(numInstances))
	}

	if numInstances > 0 {
		for i := 0; int(gpuCount) > i; i++ {
			var migGeometry []*migInstance = make([]*migInstance, int(numInstances))
			migGeometry[0] = newMigInstance()
			migGeometry[0].setInfoMigInstance(int(numInstances), int(memoryGpu), int(fp32Gpu), maxAvailabilityGpu)
			gpus[i].setGpuSpecMigOnly(migGeometry)
		}
	}

	nodeGpus.setNodeGpus(gpus, nodeName)
	return nil
}

// Verifica si el nodo tiene recursos suficientes para el pod
func enoughNodeResources(nodeName string, availableNodeCpu int, availableNodeMem int, podRequests *framework.Resource, hardwareIsolation bool) *framework.Status {

	// if availableNodeCpu < int(podRequests.MilliCPU) {
	// 	return framework.NewStatus(framework.Unschedulable, "CPU Insuficiente")
	// }
	// if availableNodeMem < int(podRequests.Memory) {
	// 	return framework.NewStatus(framework.Unschedulable, "Memoria insuficiente")
	// }

	var gpuMemReq int = int(podRequests.ScalarResources[podRequestGpuMemory])
	var gpuFp32Req int = int(podRequests.ScalarResources[podRequestGpufp32])

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

			if hardwareIsolation && gpu.available < maxAvailabilityGpu {
				continue
			}

			gpuFp32Available, gpuMemAvailable := gpuResourcesAvailable(float64(gpu.available), float64(gpu.fp32), float64(gpu.mem))

			if gpuMemAvailable > gpuMemReq && gpuFp32Available > gpuFp32Req {
				return framework.NewStatus(framework.Success)
			}

		} else {
			var partitionsInUse []int = gpu.partitionsOccuped()
			var geometryToEvaluate int = gpu.biggestPossiblePartitionsGeometry(partitionsInUse)

			if geometryToEvaluate < 0 {
				framework.NewStatus(framework.Unschedulable, "No se ha encontrado ninguna geometría disponible")
			}

			if gpu.evaluateGeometryRequestFit(geometryToEvaluate, gpuMemReq, gpuFp32Req, hardwareIsolation) {
				return framework.NewStatus(framework.Success)
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
			var migPartition *migInstance
			var totalMigFp32 float64 = 0
			var totalMigMem float64 = 0

			for j := 0; gpu.migLength > j; j += absInt(migPartition.size) {
				migPartition = gpu.migInstances[j]
				var ratioAvail float64 = float64(migPartition.available) / float64(maxAvailabilityGpu)
				totalMigFp32 += float64(migPartition.fp32) * ratioAvail
				totalMigMem += float64(migPartition.mem) * ratioAvail
			}

			posGeometry, err := gpu.findGeometry()

			if err != nil {
				return 0, framework.NewStatus(framework.Error, err.Error())
			}

			var ratioFp32 float64 = totalMigFp32 / float64(gpu.fp32)
			var ratioMem float64 = totalMigMem / float64(gpu.mem)
			var gpuAvailable float64 = ((ratioFp32 + ratioMem) / 2.0) * float64(maxAvailabilityGpu)
			totalGpuAvailable += int(math.Round(gpuAvailable)) - MIG_PROFILES_7_RESOURCES_UNUSED[posGeometry]
		}
	}

	var res int = maxAvailabilityGpu - (totalGpuAvailable / numGpus)

	return int64(res), framework.NewStatus(framework.Success)
}

func gpuReservation(podName string, nodeName string, podRequests *framework.Resource, hardwareIsolation bool) (string, error) {

	length, err := nodeGpus.getLength(nodeName)

	if err != nil {
		return "", err
	}

	var leastAvailableValue int = maxAvailabilityGpu + 1
	var leastAvailableGpuPosition int
	var gpuAssgined int
	var fp32Req float64 = float64(podRequests.ScalarResources[podRequestGpufp32])
	var memReq float64 = float64(podRequests.ScalarResources[podRequestGpuMemory])

	for gpuPosition := 0; length > gpuPosition; gpuPosition++ {
		gpu, err := nodeGpus.getGeneralGpuResources(nodeName, gpuPosition)

		if err != nil {
			return "", err
		}

		var gpuReq int = gpuResourcesRequest(fp32Req, float64(gpu.fp32), memReq, float64(gpu.mem))

		if hardwareIsolation && gpuReq < maxAvailabilityGpu {
			gpuReq = maxAvailabilityGpu
		}

		var gpuLeft int = gpu.available - gpuReq

		if gpu.available < leastAvailableValue && gpuLeft >= 0 {
			leastAvailableValue = gpu.available
			leastAvailableGpuPosition = gpuPosition
			gpuAssgined = gpuReq
		}
	}

	if leastAvailableGpuPosition < 0 || leastAvailableGpuPosition >= length || gpuAssgined <= 0 {
		return "", fmt.Errorf("no se ha encontrado adecuadamente una gpu para realizar la reserva")
	}

	err = podsUsage.setPodResourcesGpuOnly(podName, nodeName, leastAvailableGpuPosition, gpuAssgined)

	if err != nil {
		return "", err
	}

	return fmt.Sprintf(`"%s":"%d","%s":"%d"`, GPU_POS_ANNOTATION, leastAvailableGpuPosition, MIG_SIZE_ANNOTATION, NO_MIG_INSTANCE_ASSIGNED), nil
}

func migReservation(podName string, nodeName string, podRequests *framework.Resource, hardwareIsolation bool) (string, error) {

	var defMigUseReq int
	var defGeometryRow int
	var defGpuPosition int
	var defMigPosition int
	var leastGpuReq int = maxAvailabilityGpu + 1
	var leastMigLeft int = maxAvailabilityGpu + 1
	length, err := nodeGpus.getLength(nodeName)

	if err != nil {
		return "", err
	}

	for gpuPosition := 0; length > gpuPosition; gpuPosition++ {
		gpu, err := nodeGpus.getGeneralGpuResources(nodeName, gpuPosition)

		if err != nil {
			return "", err
		}

		var partitionsInUse []int = gpu.partitionsOccuped()

		var geometriesAvailable []int = gpu.possibleGeometries(partitionsInUse)

		geometryRow, migPosition, migLeft, gpuReq, migReq := gpu.bestGeometryForMig7(geometriesAvailable, podRequests, hardwareIsolation)

		if leastGpuReq > gpuReq || (leastGpuReq == gpuReq && leastMigLeft > migLeft) {
			defGpuPosition = gpuPosition
			defMigPosition = migPosition
			leastMigLeft = migLeft
			leastGpuReq = gpuReq
			defGeometryRow = geometryRow
			defMigUseReq = migReq
		}
	}

	if defMigPosition < 0 || defMigPosition >= MIG_7_COLUMNS || defGpuPosition < 0 || defGpuPosition >= length || defGeometryRow < 0 || defGeometryRow >= MIG_7_ROWS || leastMigLeft < 0 || leastMigLeft > maxAvailabilityGpu+1 || leastGpuReq < 0 || leastGpuReq > maxAvailabilityGpu+1 {
		return "", fmt.Errorf("no se ha encontrado adecuadamente una instancia MIG para realizar la reserva")
	}

	gpu, err := nodeGpus.getGeneralGpuResources(nodeName, defGpuPosition)

	if err != nil {
		return "", err
	}

	gpu.reconfiguration(defGeometryRow)
	nodeGpus.setSingleNodeGpu(gpu, nodeName, defGpuPosition)
	podsUsage.setPodResourcesMigOnly(podName, nodeName, defGpuPosition, defMigPosition, defMigUseReq)

	return fmt.Sprintf(`"%s":"%d","%s":"%d"`, GPU_POS_ANNOTATION, defGpuPosition, MIG_SIZE_ANNOTATION, gpu.migInstances[defMigPosition].size), nil
}

func gpuResourcesAvailable(availability float64, gpuFp32 float64, gpuMem float64) (fp32Left int, memLeft int) {
	var gpuAvailable float64 = availability / float64(maxAvailabilityGpu)
	var gpuMemAvailable int = int(gpuAvailable * gpuMem)
	var gpuFp32Available int = int(gpuAvailable * gpuFp32)

	return gpuFp32Available, gpuMemAvailable
}

func gpuResourcesRequest(fp32Req float64, gpuFp32 float64, memReq float64, gpuMem float64) int {
	var fp32Ratio float64 = fp32Req / gpuFp32
	var memRatio float64 = memReq / gpuMem
	var gpuReqRatio float64 = math.Max(fp32Ratio, memRatio)
	var gpuReq float64 = math.Ceil(gpuReqRatio * float64(maxAvailabilityGpu)) // Como mínimo se debe dar los pedido por el pod

	return int(gpuReq)
}

func migResourcesToGpuResources(migUse float64, migFp32 float64, migMem float64, gpuFp32 float64, gpuMem float64) int {
	var migReqRatio float64 = migUse / float64(maxAvailabilityGpu)
	var fp32Req float64 = migReqRatio * migFp32
	var memReq float64 = migReqRatio * migMem
	var gpuFp32Ratio float64 = fp32Req / gpuFp32
	var gpuMemRatio float64 = memReq / gpuMem
	var gpuReq float64 = ((gpuFp32Ratio + gpuMemRatio) / 2.0) * float64(maxAvailabilityGpu)

	// No se utiliza el Ceil para prevenir redondeos: 7.00000000001 -> 8 los cuales suele pasar cuando la relación es exacta y float64 no es 100% preciso
	return int(math.Round(gpuReq))
}
