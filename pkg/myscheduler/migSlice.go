package myscheduler

import (
	"sync"
)

// Informacion de la particion de MIG
type migInstance struct {
	sync.RWMutex
	available int // sobre maxAvailabilityGpu
	size      int
	mem       int
	fp32      int // GFLOPS
}

// InstanciaMigNoDisponible {
// 	available: 	max
// 	size:		-size
// 	mem      	0
// 	fp32     	0
// }

// Metodos migInstance
func newMigInstance() *migInstance {
	return &migInstance{}
}

// Setters. Prohibido usarlos en nodeGpus, uso unicamente en estructuras locales
func (m *migInstance) setInfoMigInstance(size int, mem int, fp32 int, availability int) {
	m.available = availability
	m.size = size
	m.mem = mem
	m.fp32 = fp32
}
