package parquet

import (
	"encoding/gob"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/parquet-go/parquet-go/format"
)

func RecoverFromCheckpoint(dataPath, checkpointPath string, options ...WriterOption) (*CheckpointableWriter, *os.File, error) {
	checkpoint := &WriterCheckpoint{}
	if err := checkpoint.Load(checkpointPath); err != nil {
		return nil, nil, fmt.Errorf("failed to load checkpoint: %w", err)
	}

	dataFile, err := os.OpenFile(dataPath, os.O_WRONLY, 0644)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open data file: %w", err)
	}

	if _, err := dataFile.Seek(checkpoint.CurrentOffset, io.SeekStart); err != nil {
		return nil, nil, fmt.Errorf("failed to seek to checkpoint offset: %w", err)
	}

	if err := dataFile.Truncate(checkpoint.CurrentOffset); err != nil {
		return nil, nil, fmt.Errorf("failed to truncate data file to checkpoint offset: %w", err)
	}

	writer := NewCheckpointableWriter(dataFile, checkpointPath, options...)
	writer.writer.createdBy = checkpoint.CreatedBy
	writer.writer.metadata = checkpoint.Metadata
	if writer.writer.metadata == nil {
		writer.writer.metadata = make([]format.KeyValue, 0)
	}
	writer.writer.columnOrders = checkpoint.ColumnOrders
	writer.writer.schemaElements = checkpoint.SchemaElements
	writer.writer.sortingColumns = checkpoint.SortingColumns
	writer.writer.rowGroups = checkpoint.RowGroups
	writer.writer.columnIndexes = checkpoint.ColumnIndexes
	writer.writer.offsetIndexes = checkpoint.OffsetIndexes
	writer.writer.fileWriter.offset = checkpoint.CurrentOffset

	return writer, dataFile, nil
}

type CheckpointableWriter struct {
	*Writer
	dataFile       *os.File
	checkpointPath string
}

func NewCheckpointableWriter(dataFile *os.File, checkpointPath string, options ...WriterOption) *CheckpointableWriter {
	writer := NewWriter(dataFile, options...)

	return &CheckpointableWriter{
		Writer:         writer,
		dataFile:       dataFile,
		checkpointPath: checkpointPath,
	}
}

func (w *CheckpointableWriter) Checkpoint() error {
	if err := w.Writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush writer before checkpoint: %w", err)
	}

	if err := w.dataFile.Sync(); err != nil {
		return fmt.Errorf("failed to sync data file before checkpoint: %w", err)
	}

	checkpoint := &WriterCheckpoint{
		CreatedBy:      w.writer.createdBy,
		Metadata:       make([]format.KeyValue, len(w.writer.metadata)),
		ColumnOrders:   make([]format.ColumnOrder, len(w.writer.columnOrders)),
		SchemaElements: make([]format.SchemaElement, len(w.writer.schemaElements)),
		SortingColumns: make([]format.SortingColumn, len(w.writer.sortingColumns)),
		RowGroups:      make([]format.RowGroup, len(w.writer.rowGroups)),
		ColumnIndexes:  make([][]format.ColumnIndex, len(w.writer.columnIndexes)),
		OffsetIndexes:  make([][]format.OffsetIndex, len(w.writer.offsetIndexes)),
		CurrentOffset:  w.writer.fileWriter.offset,
		CreatedAt:      time.Now(),
	}

	copy(checkpoint.Metadata, w.writer.metadata)
	copy(checkpoint.ColumnOrders, w.writer.columnOrders)
	copy(checkpoint.SchemaElements, w.writer.schemaElements)
	copy(checkpoint.SortingColumns, w.writer.sortingColumns)
	copy(checkpoint.RowGroups, w.writer.rowGroups)

	for i := range w.writer.columnIndexes {
		checkpoint.ColumnIndexes[i] = make([]format.ColumnIndex, len(w.writer.columnIndexes[i]))
		copy(checkpoint.ColumnIndexes[i], w.writer.columnIndexes[i])
	}

	for i := range w.writer.offsetIndexes {
		checkpoint.OffsetIndexes[i] = make([]format.OffsetIndex, len(w.writer.offsetIndexes[i]))
		copy(checkpoint.OffsetIndexes[i], w.writer.offsetIndexes[i])
	}

	return checkpoint.Save(w.checkpointPath)
}

type WriterCheckpoint struct {
	// Writere state
	CreatedBy      string
	Metadata       []format.KeyValue
	ColumnOrders   []format.ColumnOrder
	SchemaElements []format.SchemaElement
	SortingColumns []format.SortingColumn

	// Completed row groups
	RowGroups     []format.RowGroup
	ColumnIndexes [][]format.ColumnIndex
	OffsetIndexes [][]format.OffsetIndex

	CurrentOffset int64

	CreatedAt time.Time
}

func (cp *WriterCheckpoint) Save(path string) error {
	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("failed to create checkpoint file: %w", err)
	}
	defer file.Close()

	encoder := gob.NewEncoder(file)
	if err := encoder.Encode(cp); err != nil {
		return fmt.Errorf("failed to encode checkpoint: %w", err)
	}

	if err := file.Sync(); err != nil {
		return fmt.Errorf("failed to sync checkpoint file: %w", err)
	}

	return nil
}

func (cp *WriterCheckpoint) Load(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("failed to open checkpoint file: %w", err)
	}
	defer file.Close()

	decoder := gob.NewDecoder(file)
	if err := decoder.Decode(cp); err != nil {
		return fmt.Errorf("failed to decode checkpoint: %w", err)
	}

	return nil
}
