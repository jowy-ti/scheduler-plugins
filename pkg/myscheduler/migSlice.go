package myscheduler

import (
	"sync"
)

// Informacion de la particion de MIG
type migSlice struct {
	sync.RWMutex
	available int // sobre maxAvailabilityGpu
	size      int
	mem       int
	fp32      int // GFLOPS
}

// Metodos migSlice
func newMigSlice() *migSlice {
	return &migSlice{}
}

// Setters. Prohibido usarlos en nodeGpus, uso unicamente en estructuras locales
func (m *migSlice) setInfoMigSlice(size int, mem int, fp32 int) {
	m.available = maxAvailabilityGpu
	m.size = size
	m.mem = mem
	m.fp32 = fp32
}
