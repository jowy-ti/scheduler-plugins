package myscheduler

import (
	"fmt"
	"sync"
	"time"

	"k8s.io/klog/v2"
	framework "k8s.io/kubernetes/pkg/scheduler/framework"
)

var INSTANCE_CREATION_TIME = map[int]float64{
	1: 0.16,
	2: 0.21,
	3: 0.33,
	4: 0.38,
	7: 0.42,
}

var INSTANCE_DESTRUCTION_TIME = map[int]float64{
	1: 0.21,
	2: 0.23,
	3: 0.25,
	4: 0.26,
	7: 0.26,
}

// Informacion de GPU
type gpuSpec struct {
	sync.RWMutex
	available    int // sobre maxAvailabilityGpu
	mem          int
	fp32         int // GFLOPS
	migLength    int
	migInstances []*migInstance
}

// Metodos gpuSpec
func newGpuSpec() *gpuSpec {
	return &gpuSpec{}
}

// Metodos privados para 'allNodesGpus'
func (g *gpuSpec) getMigInstance(migPosition int) (*migInstance, error) {

	var migInstances []*migInstance = g.migInstances

	if migPosition >= len(migInstances) || migPosition < 0 {
		return nil, fmt.Errorf("gpuSpec.getMigInstance: posición de MIG fuera de rango %d", migPosition)
	}

	if migInstances[migPosition] == nil {
		return nil, fmt.Errorf("gpuSpec.getMigInstance: no es la posición de ninguna partición MIG %d", migPosition)
	}

	return migInstances[migPosition], nil
}

func (g *gpuSpec) deepCopy() (*gpuSpec, error) {

	g.RLock()
	defer g.RUnlock()

	var migInstances []*migInstance

	if g.migLength > 0 {
		migInstances = make([]*migInstance, g.migLength)

		for i := 0; g.migLength > i; i += absInt(migInstances[i].size) {
			migInst, err := g.getMigInstance(i)

			if err != nil {
				return nil, err
			}

			migInst.RLock()
			migInstances[i] = &migInstance{
				available: migInst.available,
				mem:       migInst.mem,
				size:      migInst.size,
				fp32:      migInst.fp32,
			}
			migInst.RUnlock()
		}
	}

	var gpu *gpuSpec = &gpuSpec{
		available:    g.available,
		mem:          g.mem,
		fp32:         g.fp32,
		migLength:    g.migLength,
		migInstances: migInstances,
	}

	return gpu, nil
}

// Unicamente usar los siguientes métodos en estructuras locales

// Añade la información general
func (g *gpuSpec) setGpuSpecGpuOnly(mem int, fp32 int, migLength int) error {
	if migLength > 0 {
		g.available = -1
	} else {
		g.available = maxAvailabilityGpu
	}
	g.mem = mem
	g.fp32 = fp32
	g.migLength = migLength
	return nil
}

// Añade la informacion especifica de MIG
func (g *gpuSpec) setGpuSpecMigOnly(migInstance []*migInstance) error {
	if migInstance == nil {
		return fmt.Errorf("gpuSpec.setGpuSpecMigOnly: no se puede crear un *gpuSpec con mig y migInstance nil")
	}
	g.migInstances = migInstance
	return nil
}

// Encuentra la geometria actual de la GPU con MIG
func (g *gpuSpec) findGeometry() (geometry int, err error) {

	if g.migLength <= 0 {
		return -1, fmt.Errorf("gpuSpec.findGeometry: no se puede encontrar la geometría porque el valor de migLength es inválido")
	}

	for i := 0; MIG_7_ROWS > i; i++ {
		var migInstance *migInstance
		var geometryFinded bool = true

		for j := 0; g.migLength > j; j += absInt(migInstance.size) {
			migInstance = g.migInstances[j]

			if MIG_PROFILES_7_INSTANCES[i][j] != migInstance.size {
				geometryFinded = false
				break
			}
		}
		if geometryFinded {
			return i, nil
		}
	}

	return -1, fmt.Errorf("gpuSpec.findGeometry: no se ha encontrado la geometría MIG para la gpu")
}

// Devuelve una lista de las posiciones MIG ocupadas para una GPU
func (g *gpuSpec) partitionsOccuped() []int {

	var usedPartitions []int = make([]int, 0)

	var migInstance *migInstance
	for migPosition := 0; g.migLength > migPosition; migPosition += absInt(migInstance.size) {
		migInstance = g.migInstances[migPosition]

		if migInstance.available < maxAvailabilityGpu {
			usedPartitions = append(usedPartitions, migPosition)
		}
	}

	return usedPartitions
}

// Encuentra la geometría con las particiones más grandes, dado el orden de las geometrias, es la primera que encuentra
func (g *gpuSpec) biggestPossiblePartitionsGeometry(partitionsInUse []int) int {

	var lenghtPartitionsInUse int = len(partitionsInUse)
	var biggestPartitionGeometry int = 0

	if lenghtPartitionsInUse == 0 {
		return biggestPartitionGeometry
	}

	for i := 0; MIG_7_ROWS > i; i++ {
		var okGeometry bool = true

		for j := 0; lenghtPartitionsInUse > j; j++ {
			var migPos int = partitionsInUse[j]
			var migSize int = g.migInstances[migPos].size

			if MIG_PROFILES_7_INSTANCES[i][migPos] != migSize {
				okGeometry = false
				break
			}
		}

		if okGeometry {
			return i
		}
	}
	return -1
}

// Evalua si la geometria tiene alguna partición que cumpla con los requisistos de memoria y fp32 del pod
func (g *gpuSpec) evaluateGeometryRequestFit(geometryToEvaluate int, memReq int, fp32Req int, hardwareIsolation bool) bool {

	var migSizeGeometry int

	for i := 0; MIG_7_COLUMNS > i; i += absInt(migSizeGeometry) {
		var migFp32 float64
		var migMem float64
		var migAvailable int
		migSizeGeometry = MIG_PROFILES_7_INSTANCES[geometryToEvaluate][i]

		if g.migInstances[i] != nil && g.migInstances[i].size == migSizeGeometry {
			var migInstance *migInstance = g.migInstances[i]
			migAvailable = migInstance.available
			migFp32 = float64(migInstance.fp32)
			migMem = float64(migInstance.mem)

			if hardwareIsolation && migAvailable < maxAvailabilityGpu {
				continue
			}

		} else {
			migAvailable = maxAvailabilityGpu
			migFp32 = MIG_7_COMPUTE_FRACTION[migSizeGeometry] * float64(g.fp32)
			migMem = MIG_7_MEMORY_FRACTION[migSizeGeometry] * float64(g.mem)
		}

		gpuFp32Left, gpuMemLeft := gpuResourcesAvailable(float64(migAvailable), migFp32, migMem)

		if gpuMemLeft >= memReq && gpuFp32Left >= fp32Req {
			return true
		}
	}
	return false
}

// Devuelve una lista de las filas que representan las posibles geometrias, dadas unas particiones en uso inmutables
func (g *gpuSpec) possibleGeometries(partitionsInUse []int) []int {

	var geometriesAvailable []int = make([]int, 0)
	var lenghtPartitionsInUse int = len(partitionsInUse)

	if lenghtPartitionsInUse == 0 {

		for i := 0; MIG_7_ROWS > i; i++ {
			geometriesAvailable = append(geometriesAvailable, i)
		}
	} else {
		for i := 0; MIG_7_ROWS > i; i++ {
			var okGeometry bool = true

			for j := 0; lenghtPartitionsInUse > j; j++ {
				var migPos int = partitionsInUse[j]
				var migSizeGeometry int = MIG_PROFILES_7_INSTANCES[i][migPos]
				var migSizeGpu int = g.migInstances[migPos].size

				if migSizeGeometry != migSizeGpu {
					okGeometry = false
					break
				}
			}

			if okGeometry {
				geometriesAvailable = append(geometriesAvailable, i)
			}
		}
	}
	return geometriesAvailable
}

// Esta función se ha hecho pensando en pofiles MIG de 7 instancias
func (g *gpuSpec) bestGeometryForMig7(geometries []int, podRequests *framework.Resource, hardwareIsolation bool) (geometryRow int, migPosition int, migLeft int, gpuReq int, migReq int) {

	var fp32Req float64 = float64(podRequests.ScalarResources[podRequestGpufp32])
	var memReq float64 = float64(podRequests.ScalarResources[podRequestGpuMemory])

	var leastMigLeft int = maxAvailabilityGpu + 1
	var leastGpuReq int = maxAvailabilityGpu + 1
	var defGeometry int = -1
	var defMigPosition int = -1
	var defMigReq int = -1

	var geometriesLength int = len(geometries)

	for i := 0; geometriesLength > i; i++ {
		var geometryPos int = geometries[i]
		var migSizeGeometry int

		for j := 0; MIG_7_COLUMNS > j; j += absInt(migSizeGeometry) {
			var migFp32 float64
			var migMem float64
			var migAvailable int
			migSizeGeometry = MIG_PROFILES_7_INSTANCES[geometryPos][j]

			if migSizeGeometry == invalidInstanceSize {
				continue
			}

			if g.migInstances[j] != nil && g.migInstances[j].size == migSizeGeometry {
				var migInstance *migInstance = g.migInstances[j]
				migAvailable = migInstance.available
				migFp32 = float64(migInstance.fp32)
				migMem = float64(migInstance.mem)
			} else {
				migAvailable = maxAvailabilityGpu
				migFp32 = MIG_7_COMPUTE_FRACTION[migSizeGeometry] * float64(g.fp32)
				migMem = MIG_7_MEMORY_FRACTION[migSizeGeometry] * float64(g.mem)
			}

			var migReq int = gpuResourcesRequest(fp32Req, migFp32, memReq, migMem)

			if hardwareIsolation && migReq < maxAvailabilityGpu {
				migReq = maxAvailabilityGpu
			}

			var migLeft int = migAvailable - migReq

			if migLeft < 0 {
				continue
			}

			// Se calcula el uso de GPU en base al de la partición mig y se suma la porción que no es posible utilizar debido a la geometría
			var gpuReq int = migResourcesToGpuResources(float64(migReq), migFp32, migMem, float64(g.fp32), float64(g.mem)) + MIG_PROFILES_7_RESOURCES_UNUSED[geometryPos]

			if leastGpuReq > gpuReq || (leastGpuReq == gpuReq && leastMigLeft > migLeft) {
				leastGpuReq = gpuReq
				leastMigLeft = migLeft
				defGeometry = geometryPos
				defMigPosition = j
				defMigReq = migReq
			}
		}
	}
	return defGeometry, defMigPosition, leastMigLeft, leastGpuReq, defMigReq
}

func (g *gpuSpec) reconfiguration(geometryRow int) {

	var waitTime float64 = 0
	var geometry [MIG_7_COLUMNS]int = MIG_PROFILES_7_INSTANCES[geometryRow]
	var migGeometry []*migInstance = make([]*migInstance, MIG_7_COLUMNS)
	var migSizeGeometry int

	for i := 0; MIG_7_COLUMNS > i; i += absInt(migSizeGeometry) {
		var availability int
		var migInstance *migInstance = g.migInstances[i]
		migSizeGeometry = geometry[i]

		if migSizeGeometry == invalidInstanceSize {
			continue
		}

		if migInstance != nil && migSizeGeometry == migInstance.size {
			availability = migInstance.available
		} else {
			availability = maxAvailabilityGpu
			waitTime += INSTANCE_CREATION_TIME[migSizeGeometry]
			if migInstance != nil {
				for j := i; i+migSizeGeometry > j; j++ {
					if g.migInstances[j] != nil {
						waitTime += INSTANCE_DESTRUCTION_TIME[g.migInstances[j].size]
					}
				}
			}
		}

		var migMemory float64 = float64(g.mem) * MIG_7_MEMORY_FRACTION[migSizeGeometry]
		var migFp32 float64 = float64(g.fp32) * MIG_7_COMPUTE_FRACTION[migSizeGeometry]

		migGeometry[i] = newMigInstance()
		migGeometry[i].setInfoMigInstance(migSizeGeometry, int(migMemory), int(migFp32), availability)
	}
	g.setGpuSpecMigOnly(migGeometry)
	klog.V(0).Infof("WaitTime: %f", waitTime)
	time.Sleep(time.Duration(waitTime * float64(time.Second)))
}

// Unicamente usar los siguientes métodos en estructuras locales EOF
