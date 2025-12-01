package myscheduler

import (
	"sync"
)

// Informacion de la particion de MIG
type migSlice struct {
	sync.RWMutex
	available int
	size      int
	mem       int
	fp32      int // GFLOPS
}

// Metodos migSlice
func newMigSlice() *migSlice {
	return &migSlice{}
}

// Setters
func (m *migSlice) setInfoMigSlice(size int, mem int, fp32 int) {
	m.available = maxAvailabilityGpu
	m.size = size
	m.mem = mem
	m.fp32 = fp32
}
