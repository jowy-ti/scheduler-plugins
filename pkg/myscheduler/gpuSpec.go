package myscheduler

import (
	"fmt"
	"sync"

	framework "k8s.io/kubernetes/pkg/scheduler/framework"
)

// Informacion de GPU
type gpuSpec struct {
	sync.RWMutex
	available int // sobre maxAvailabilityGpu
	mem       int
	fp32      int // GFLOPS
	migLength int
	migSlices []*migSlice
}

// Metodos gpuSpec
func newGpuSpec() *gpuSpec {
	return &gpuSpec{}
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
func (g *gpuSpec) setGpuSpecMigOnly(migPartition []*migSlice) error {
	if migPartition == nil {
		return fmt.Errorf("gpuSpec.setGpuSpecMigOnly: no se puede crear un *gpuSpec con mig y migPartition nil")
	}
	g.migSlices = migPartition
	return nil
}

// Devuelve una lista de las posiciones MIG ocupadas para una GPU
func (g *gpuSpec) partitionsOccuped() []int {

	var usedPartitions []int = make([]int, 0)

	for migPosition := 0; g.migLength > migPosition; migPosition++ {
		var migPartition *migSlice = g.migSlices[migPosition]

		if migPartition.available < maxAvailabilityGpu {
			usedPartitions = append(usedPartitions, migPosition)
		}

		migPosition += migPartition.size - 1
	}

	return usedPartitions
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
				var migSize int = g.migSlices[migPos].size

				if MIG_PROFILES_7_INSTANCES[i][migPos] != migSize {
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
func (g *gpuSpec) bestGeometryForMig7(geometries []int, podRequests *framework.Resource) (geometryRow int, migPosition int, migLeft int, gpuReq int, migReq int) {

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
		// klog.V(0).Infof("Geometry: %d", geometryPos)

		for j := 0; MIG_7_COLUMNS > j; j++ {
			var migFp32 float64
			var migMem float64
			var migAvailable int
			var migSize int = MIG_PROFILES_7_INSTANCES[geometryPos][j]

			if migSize == invalidInstanceSize {
				continue
			}

			if g.migSlices[j] != nil && g.migSlices[j].size == migSize {
				var migPartition *migSlice = g.migSlices[j]
				migAvailable = migPartition.available
				migFp32 = float64(migPartition.fp32)
				migMem = float64(migPartition.mem)
			} else {
				migAvailable = maxAvailabilityGpu
				migFp32 = MIG_7_COMPUTE_FRACTION[migSize] * float64(g.fp32)
				migMem = MIG_7_MEMORY_FRACTION[migSize] * float64(g.mem)
			}
			// klog.V(0).Infof("- migPartition: %d", j)
			var migReq int = gpuResourcesRequest(fp32Req, migFp32, memReq, migMem)
			// klog.V(0).Infof("  - migReq: %d", migReq)
			// Se calcula el uso de GPU en base al de la partición mig y se suma la porción que no es posible utilizar debido a la geometría
			var gpuReq int = migResourcesToGpuResources(migReq, migFp32, migMem, g.fp32, g.mem) + MIG_PROFILES_7_RESOURCES_UNUSED[geometryPos]
			// klog.V(0).Infof("  - gpuReq: %d", gpuReq)
			var migLeft int = migAvailable - migReq
			// klog.V(0).Infof("  - migLeft: %d", migLeft)

			if migLeft >= 0 {
				if leastGpuReq > gpuReq || (leastGpuReq == gpuReq && leastMigLeft > migLeft) {
					leastGpuReq = gpuReq
					leastMigLeft = migLeft
					defGeometry = geometryPos
					defMigPosition = j
					defMigReq = migReq
				}
			}

			j += migSize - 1
		}
	}
	return defGeometry, defMigPosition, leastMigLeft, leastGpuReq, defMigReq
}

func (g *gpuSpec) reconfiguration(geometryRow int) {

	var geometry [MIG_7_COLUMNS]int = MIG_PROFILES_7_INSTANCES[geometryRow]
	var migGeometry []*migSlice = make([]*migSlice, MIG_7_COLUMNS)

	for i := 0; MIG_7_COLUMNS > i; i++ {
		var migMemory float64
		var migFp32 float64
		var availability int
		var migSize int = geometry[i]

		if migSize == invalidInstanceSize {
			continue
		}

		if g.migSlices[i] != nil && migSize == g.migSlices[i].size {
			availability = g.migSlices[i].available
		} else {
			availability = maxAvailabilityGpu
		}

		migMemory = float64(g.mem) * MIG_7_MEMORY_FRACTION[migSize]
		migFp32 = float64(g.fp32) * MIG_7_COMPUTE_FRACTION[migSize]

		migGeometry[i] = newMigSlice()
		migGeometry[i].setInfoMigSlice(migSize, int(migMemory), int(migFp32), availability)
		i += migSize - 1
	}
	g.setGpuSpecMigOnly(migGeometry)
}

// Unicamente usar los siguientes métodos en estructuras locales EOF

// Metodos privados para 'allNodesGpus'
func (g *gpuSpec) getMigSlice(migPosition int) (*migSlice, error) {

	var migSlices []*migSlice = g.migSlices

	if migPosition >= len(migSlices) || migPosition < 0 {
		return nil, fmt.Errorf("gpuSpec.getMigSlice: posición de MIG fuera de rango %d", migPosition)
	}

	if migSlices[migPosition] == nil {
		return nil, fmt.Errorf("gpuSpec.getMigSlice: no es la posición de ninguna partición MIG %d", migPosition)
	}

	return migSlices[migPosition], nil
}

func (g *gpuSpec) deepCopy() (*gpuSpec, error) {

	g.RLock()
	defer g.RUnlock()

	var migPartitions []*migSlice

	if g.migLength > 0 {
		migPartitions = make([]*migSlice, g.migLength)

		for i := 0; g.migLength > i; i++ {
			migPartition, err := g.getMigSlice(i)

			if err != nil {
				return nil, err
			}

			migPartition.RLock()
			migPartitions[i] = &migSlice{
				available: migPartition.available,
				mem:       migPartition.mem,
				size:      migPartition.size,
				fp32:      migPartition.fp32,
			}
			migPartition.RUnlock()
			i += migPartitions[i].size - 1
		}
	}

	var gpu *gpuSpec = &gpuSpec{
		available: g.available,
		mem:       g.mem,
		fp32:      g.fp32,
		migLength: g.migLength,
		migSlices: migPartitions,
	}

	return gpu, nil
}
