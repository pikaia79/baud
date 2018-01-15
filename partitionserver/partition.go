package partitionserver

type Partition struct {
	store *store.Store

	space int
	startPID, endPID int

	role int
}


