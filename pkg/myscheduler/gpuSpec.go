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

// Unicamente usarlo en estructuras locales

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

		for i := 0; MIG_PROFILES_7_INSTANCES_ROWS > i; i++ {
			geometriesAvailable = append(geometriesAvailable, i)
		}
	} else {
		for i := 0; MIG_PROFILES_7_INSTANCES_ROWS > i; i++ {
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

func (g *gpuSpec) bestGeometry(geometries []int, partitionsInUse []int, podRequests *framework.Resource) (geometryRow int, migPosition int, migUsage int, availableGpu int) {

	var geometriesLength int = len(geometries)
	var fp32Req int = int(podRequests.ScalarResources[podRequestGpufp32])
	var memReq int = int(podRequests.ScalarResources[podRequestGpuMemory])
	var defGeometry int = -1
	var defMigPosition int = -1
	var migInstances int = MIG_PROFILES_7_INSTANCES_COLUMNS

	for i := 0; geometriesLength > i; i++ {
		var geometryPos int = geometries[i]
		var indexPartitionInUse int = 0

		for j := 0; MIG_PROFILES_7_INSTANCES_COLUMNS > j; j++ {
			var migSize int = MIG_PROFILES_7_INSTANCES[geometryPos][j]

			if migSize == invalidInstanceSize {
				continue
			}

			if partitionsInUse[indexPartitionInUse] == j {

			} else {
				var ratioSize float64 = float64(migSize) / float64(migInstances)

			}

			j += migSize - 1
		}
	}

	return 0, 0, 0, 0
}

func (g *gpuSpec) reconfiguration(geometryRow int) {

}

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
