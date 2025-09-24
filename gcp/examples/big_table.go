package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"cloud.google.com/go/bigtable"
	"github.com/joho/godotenv"
)

type Config struct {
	ProjectID    string
	InstanceID   string
	TableID      string
	ColumnFamily string
}

// ----------------------
// Utility
// ----------------------

// Load environment variables from .env
func loadConfig() Config {
	if err := godotenv.Load(); err != nil {
		log.Fatalf("Error loading .env file: %v", err)
	}

	return Config{
		ProjectID:    os.Getenv("PROJECT_ID"),
		InstanceID:   os.Getenv("INSTANCE_ID"),
		TableID:      os.Getenv("TABLE_ID"),
		ColumnFamily: os.Getenv("COLUMN_FAMILY"),
	}
}

// Generate a row key using reversed timestamp to avoid hotspotting
func rowKey(deviceID string, t time.Time) string {
	reversed := ^uint64(uint64(t.UnixMilli()))
	return fmt.Sprintf("%s#%d", deviceID, reversed)
}

// ----------------------
// Bigtable operations
// ----------------------

// Create and return a Bigtable client
func createBigtableClient(ctx context.Context, cfg Config) *bigtable.Client {
	client, err := bigtable.NewClient(ctx, cfg.ProjectID, cfg.InstanceID)
	if err != nil {
		log.Fatalf("Failed to create Bigtable client: %v", err)
	}
	return client
}

// Write a new row
func writeRow(ctx context.Context, tbl *bigtable.Table, cfg Config, deviceID string) string {
	key := rowKey(deviceID, time.Now())
	mut := bigtable.NewMutation()
	mut.Set(cfg.ColumnFamily, "temp_c", bigtable.Now(), []byte("27.4"))
	mut.Set(cfg.ColumnFamily, "hum_pct", bigtable.Now(), []byte("61"))

	if err := tbl.Apply(ctx, key, mut); err != nil {
		log.Fatalf("Failed to write row: %v", err)
	}
	fmt.Println("Wrote row:", key)
	return key
}

// Read a single row by key
func readRow(ctx context.Context, tbl *bigtable.Table, key string) {
	r, err := tbl.ReadRow(ctx, key)
	if err != nil {
		log.Fatalf("Failed to read row: %v", err)
	}

	fmt.Println("Reading row:", key)
	for fam, items := range r {
		fmt.Println("Family:", fam)
		for _, it := range items {
			fmt.Printf("  %s @%v = %s\n", it.Column, it.Timestamp, string(it.Value))
		}
	}
}

// Scan all rows with a specific prefix
func scanRows(ctx context.Context, tbl *bigtable.Table, prefix string) {
	fmt.Println("Scanning rows with prefix:", prefix)
	rt := bigtable.PrefixRange(prefix)

	err := tbl.ReadRows(ctx, rt,
		func(r bigtable.Row) bool {
			fmt.Println("Row:", r.Key())
			// readRow(ctx, tbl, r.Key())
			return true // continue scanning
		},
		bigtable.RowFilter(bigtable.LatestNFilter(1)), // only latest version
	)
	if err != nil {
		log.Fatalf("Failed to scan rows: %v", err)
	}
}

// ----------------------
// Main
// ----------------------
func main() {
	// Load configuration
	cfg := loadConfig()

	ctx := context.Background()
	client := createBigtableClient(ctx, cfg)
	defer client.Close()

	tbl := client.Open(cfg.TableID)

	// Run operations
	rowKey := writeRow(ctx, tbl, cfg, "sensor-42")

	readRow(ctx, tbl, rowKey)

	scanRows(ctx, tbl, "sensor-42#")
}
