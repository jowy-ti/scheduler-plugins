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

// Setters. Prohibido usarlos en nodeGpus, uso unicamente en estructuras locales
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

func (g *gpuSpec) setGpuSpecMigOnly(migPartition []*migSlice) error {
	if migPartition == nil {
		return fmt.Errorf("gpuSpec.setGpuSpecMigOnly: no se puede crear un *gpuSpec con mig y migPartition nil")
	}
	g.migSlices = migPartition
	return nil
}

// Informacion
// Se devuelve una lista de las posiciones MIG ocupadas para una GPU
func (g *gpuSpec) partitionsOccuped() []int {
	return nil
}

func (g *gpuSpec) bestGeometry(geometries []int, podRequests *framework.Resource) (geometryRow int, migPosition int, migUsage int, availableGpu int) {
	return 0, 0, 0, 0
}

// Modificadores
func (g *gpuSpec) reconfiguration(geometryRow int) {

}

// Metodos privados para 'allNodesGpus' no protegidos con RWMutex
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
