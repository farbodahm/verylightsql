package main

import (
	"fmt"
	"math/rand"
	"os"
	"testing"
)

// setupBenchmarkTable creates a temporary database for benchmarking
func setupBenchmarkTable(b *testing.B) (*Table, func()) {
	b.Helper()
	tmpFile, err := os.CreateTemp("", "benchmark_*.db")
	if err != nil {
		b.Fatal(err)
	}
	tmpFile.Close()

	table, err := OpenDatabase(tmpFile.Name())
	if err != nil {
		os.Remove(tmpFile.Name())
		b.Fatal(err)
	}

	cleanup := func() {
		table.Close()
		os.Remove(tmpFile.Name())
	}

	return table, cleanup
}

// createRow creates a test row with the given ID
func createRow(id int32) *Row {
	row := &Row{ID: id}
	copy(row.Username[:], fmt.Sprintf("user%d", id))
	copy(row.Email[:], fmt.Sprintf("user%d@example.com", id))
	return row
}

// populateTable inserts n rows with sequential IDs starting from 0
func populateTable(b *testing.B, table *Table, n int) {
	b.Helper()
	for i := range n {
		if err := table.Insert(createRow(int32(i))); err != nil {
			b.Fatal(err)
		}
	}
}

// maxSafeRows returns the maximum number of rows we can safely insert
// given tableMaxPages and the B-tree structure.
// With LeafNodeMaxCells ≈ 13 and some pages used for internal nodes,
// we conservatively estimate ~80% of pages can be leaves.
func maxSafeRows() int {
	return int(float64(tableMaxPages) * 0.8 * float64(LeafNodeMaxCells))
}

func BenchmarkInsert(b *testing.B) {
	b.Run("Sequential", func(b *testing.B) {
		// Each iteration inserts into a fresh table to avoid page limit
		for range b.N {
			b.StopTimer()
			table, cleanup := setupBenchmarkTable(b)
			b.StartTimer()

			// Insert a batch of rows (enough to trigger multiple splits)
			batchSize := 100
			for j := range batchSize {
				if err := table.Insert(createRow(int32(j))); err != nil {
					cleanup()
					b.Fatal(err)
				}
			}

			b.StopTimer()
			cleanup()
		}
	})

	b.Run("Random", func(b *testing.B) {
		// Each iteration inserts into a fresh table
		rng := rand.New(rand.NewSource(42))

		for range b.N {
			b.StopTimer()
			table, cleanup := setupBenchmarkTable(b)

			// Pre-generate random IDs
			batchSize := 100
			ids := make([]int32, batchSize)
			used := make(map[int32]bool)
			for j := range batchSize {
				for {
					id := rng.Int31()
					if !used[id] {
						used[id] = true
						ids[j] = id
						break
					}
				}
			}

			b.StartTimer()
			for _, id := range ids {
				if err := table.Insert(createRow(id)); err != nil {
					cleanup()
					b.Fatal(err)
				}
			}

			b.StopTimer()
			cleanup()
		}
	})
}

func BenchmarkFindKey(b *testing.B) {
	b.Run("Shallow_50rows", func(b *testing.B) {
		table, cleanup := setupBenchmarkTable(b)
		defer cleanup()

		populateTable(b, table, 50)

		b.ResetTimer()
		for i := range b.N {
			key := uint32(i % 50)
			if _, err := table.findKey(key); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("Medium_200rows", func(b *testing.B) {
		table, cleanup := setupBenchmarkTable(b)
		defer cleanup()

		populateTable(b, table, 200)

		b.ResetTimer()
		for i := range b.N {
			key := uint32(i % 200)
			if _, err := table.findKey(key); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("Deep_300rows", func(b *testing.B) {
		table, cleanup := setupBenchmarkTable(b)
		defer cleanup()

		populateTable(b, table, 300)

		b.ResetTimer()
		for i := range b.N {
			key := uint32(i % 300)
			if _, err := table.findKey(key); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkSelectAll(b *testing.B) {
	for _, rowCount := range []int{50, 100, 200} {
		b.Run(fmt.Sprintf("Rows_%d", rowCount), func(b *testing.B) {
			table, cleanup := setupBenchmarkTable(b)
			defer cleanup()

			populateTable(b, table, rowCount)

			b.ResetTimer()
			for range b.N {
				rows := table.SelectAll()
				if len(rows) != rowCount {
					b.Fatalf("expected %d rows, got %d", rowCount, len(rows))
				}
			}
		})
	}
}

func BenchmarkCursor(b *testing.B) {
	b.Run("Advance_50rows", func(b *testing.B) {
		table, cleanup := setupBenchmarkTable(b)
		defer cleanup()

		populateTable(b, table, 50)

		b.ResetTimer()
		for range b.N {
			cursor := TableStart(table)
			for !cursor.IsEndOfTable() {
				_ = cursor.Value()
				cursor.Advance()
			}
		}
	})

	b.Run("Advance_200rows", func(b *testing.B) {
		table, cleanup := setupBenchmarkTable(b)
		defer cleanup()

		populateTable(b, table, 200)

		b.ResetTimer()
		for range b.N {
			cursor := TableStart(table)
			for !cursor.IsEndOfTable() {
				_ = cursor.Value()
				cursor.Advance()
			}
		}
	})
}

func BenchmarkSerializeRow(b *testing.B) {
	row := createRow(42)
	dest := make([]byte, rowSize)

	b.ResetTimer()
	for range b.N {
		serializeRow(row, dest)
	}
}

func BenchmarkDeserializeRow(b *testing.B) {
	src := make([]byte, rowSize)
	row := createRow(42)
	serializeRow(row, src)

	var destRow Row

	b.ResetTimer()
	for range b.N {
		deserializeRow(src, &destRow)
	}
}

func BenchmarkBTreeLeafNode(b *testing.B) {
	b.Run("KeyAccess", func(b *testing.B) {
		node := make([]byte, pageSize)
		initializeLeafNode(node)
		*leafNodeNumCells(node) = uint32(LeafNodeMaxCells)

		// Initialize keys
		for i := range LeafNodeMaxCells {
			*leafNodeKey(node, uint32(i)) = uint32(i) * 10
		}

		b.ResetTimer()
		for i := range b.N {
			_ = *leafNodeKey(node, uint32(i%LeafNodeMaxCells))
		}
	})

	b.Run("ValueAccess", func(b *testing.B) {
		node := make([]byte, pageSize)
		initializeLeafNode(node)
		*leafNodeNumCells(node) = uint32(LeafNodeMaxCells)

		b.ResetTimer()
		for i := range b.N {
			_ = leafNodeValue(node, uint32(i%LeafNodeMaxCells))
		}
	})

	b.Run("CellAccess", func(b *testing.B) {
		node := make([]byte, pageSize)
		initializeLeafNode(node)
		*leafNodeNumCells(node) = uint32(LeafNodeMaxCells)

		b.ResetTimer()
		for i := range b.N {
			_ = leafNodeCell(node, uint32(i%LeafNodeMaxCells))
		}
	})
}

func BenchmarkBTreeInternalNode(b *testing.B) {
	b.Run("FindChild", func(b *testing.B) {
		node := make([]byte, pageSize)
		initializeInternalNode(node)
		*internalNodeNumKeys(node) = InternalNodeMaxKeys

		// Set up keys: 100, 200, 300
		for i := range InternalNodeMaxKeys {
			*internalNodeKey(node, uint32(i)) = uint32(i+1) * 100
		}

		b.ResetTimer()
		for i := range b.N {
			_ = internalNodeFindChild(node, uint32(i%400))
		}
	})

	b.Run("KeyAccess", func(b *testing.B) {
		node := make([]byte, pageSize)
		initializeInternalNode(node)
		*internalNodeNumKeys(node) = InternalNodeMaxKeys

		for i := range InternalNodeMaxKeys {
			*internalNodeKey(node, uint32(i)) = uint32(i+1) * 100
		}

		b.ResetTimer()
		for i := range b.N {
			_ = *internalNodeKey(node, uint32(i%InternalNodeMaxKeys))
		}
	})

	b.Run("ChildAccess", func(b *testing.B) {
		node := make([]byte, pageSize)
		initializeInternalNode(node)
		*internalNodeNumKeys(node) = InternalNodeMaxKeys

		for i := range InternalNodeMaxKeys {
			_ = *internalNodeChild(node, uint32(i))
		}

		b.ResetTimer()
		for i := range b.N {
			_ = *internalNodeChild(node, uint32(i%InternalNodeMaxKeys))
		}
	})
}

func BenchmarkLeafNodeSplit(b *testing.B) {
	// Each iteration creates a fresh table and fills one leaf to trigger a split
	b.Run("SingleSplit", func(b *testing.B) {
		for range b.N {
			b.StopTimer()
			table, cleanup := setupBenchmarkTable(b)

			// Fill leaf node to capacity (LeafNodeMaxCells)
			for j := range LeafNodeMaxCells {
				if err := table.Insert(createRow(int32(j * 2))); err != nil {
					cleanup()
					b.Fatal(err)
				}
			}

			b.StartTimer()
			// This insert triggers a leaf split
			if err := table.Insert(createRow(int32(1))); err != nil {
				cleanup()
				b.Fatal(err)
			}
			b.StopTimer()

			cleanup()
		}
	})
}

func BenchmarkInternalNodeSplit(b *testing.B) {
	// This benchmark measures the time to split an internal node
	// We need to fill the tree enough to have a full internal node, then trigger a split
	b.Run("SingleSplit", func(b *testing.B) {
		for range b.N {
			b.StopTimer()
			table, cleanup := setupBenchmarkTable(b)

			// Calculate how many rows we need to fill the internal node
			// InternalNodeMaxKeys = 3, so we need 4 leaf nodes to have a full internal node
			// Each leaf holds LeafNodeMaxCells rows
			// After 4 leaves, the next split will cause an internal node split
			rowsNeeded := LeafNodeMaxCells * (InternalNodeMaxKeys + 1)

			for j := range rowsNeeded {
				if err := table.Insert(createRow(int32(j))); err != nil {
					cleanup()
					b.Fatal(err)
				}
			}

			b.StartTimer()
			// This insert triggers an internal node split
			if err := table.Insert(createRow(int32(rowsNeeded))); err != nil {
				cleanup()
				b.Fatal(err)
			}
			b.StopTimer()

			cleanup()
		}
	})
}

func BenchmarkMixedWorkload(b *testing.B) {
	b.Run("InsertAndSearch", func(b *testing.B) {
		// Each iteration uses a fresh table with a batch of operations
		for range b.N {
			b.StopTimer()
			table, cleanup := setupBenchmarkTable(b)

			// Pre-populate with some data
			populateTable(b, table, 100)

			rng := rand.New(rand.NewSource(42))
			nextID := int32(100)

			b.StartTimer()
			// Run a batch of mixed operations
			for j := range 50 {
				if j%2 == 0 {
					// Insert
					if err := table.Insert(createRow(nextID)); err != nil {
						cleanup()
						b.Fatal(err)
					}
					nextID++
				} else {
					// Search
					key := uint32(rng.Int31n(int32(nextID)))
					if _, err := table.findKey(key); err != nil {
						cleanup()
						b.Fatal(err)
					}
				}
			}
			b.StopTimer()

			cleanup()
		}
	})
}

func BenchmarkGetNodeMaxKey(b *testing.B) {
	b.Run("LeafNode", func(b *testing.B) {
		node := make([]byte, pageSize)
		initializeLeafNode(node)
		*leafNodeNumCells(node) = uint32(LeafNodeMaxCells)
		for i := range LeafNodeMaxCells {
			*leafNodeKey(node, uint32(i)) = uint32(i) * 10
		}

		b.ResetTimer()
		for range b.N {
			_ = getNodeMaxKey(node)
		}
	})

	b.Run("InternalNode", func(b *testing.B) {
		node := make([]byte, pageSize)
		initializeInternalNode(node)
		*internalNodeNumKeys(node) = InternalNodeMaxKeys
		for i := range InternalNodeMaxKeys {
			*internalNodeKey(node, uint32(i)) = uint32(i+1) * 100
		}

		b.ResetTimer()
		for range b.N {
			_ = getNodeMaxKey(node)
		}
	})
}
