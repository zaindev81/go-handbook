package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/joho/godotenv"
	"google.golang.org/api/iterator"
)

// Row model matching your table schema.
type EventRow struct {
	EventID     string               `bigquery:"event_id"`
	DeviceID    string               `bigquery:"device_id"`
	Timestamp   time.Time            `bigquery:"timestamp"`
	Temperature bigquery.NullFloat64 `bigquery:"temperature"` // Use BigQuery's null type
}

// queryEventsTable queries the events table defined by your Terraform schema.
func queryEventsTable(projectID, datasetID, tableID string) error {
	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		return fmt.Errorf("bigquery.NewClient: %w", err)
	}
	defer client.Close()

	tableRef := fmt.Sprintf("`%s.%s.%s`", projectID, datasetID, tableID)
	queryStr := fmt.Sprintf(`
		SELECT event_id, device_id, timestamp, temperature
		FROM %s
		ORDER BY timestamp DESC
		LIMIT 10`, tableRef)

	q := client.Query(queryStr)
	it, err := q.Read(ctx)
	if err != nil {
		return fmt.Errorf("query.Read: %w", err)
	}

	fmt.Printf("Query results from %s:\n", tableRef)
	for {
		var row EventRow
		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return fmt.Errorf("iterator.Next: %w", err)
		}

		tempStr := "NULL"
		if row.Temperature.Valid {
			tempStr = fmt.Sprintf("%.2f°C", row.Temperature.Float64)
		}

		fmt.Printf("Event: %s, Device: %s, Time: %s, Temp: %s\n",
			row.EventID, row.DeviceID, row.Timestamp.Format(time.RFC3339), tempStr)
	}

	return nil
}

// insertEvents streams rows into BigQuery with InsertID for deduplication.
func insertEvents(ctx context.Context, client *bigquery.Client, datasetID, tableID string, rows []EventRow) error {
	inserter := client.Dataset(datasetID).Table(tableID).Inserter()

	// Use StructSavers so we can set InsertID (helps dedupe on retries).
	savers := make([]*bigquery.StructSaver, 0, len(rows))
	for _, r := range rows {
		savers = append(savers, &bigquery.StructSaver{
			Struct:   r,
			InsertID: r.EventID, // idempotency: same EventID won't insert twice
		})
	}

	fmt.Println("Streaming rows into BigQuery...")
	if err := inserter.Put(ctx, savers); err != nil {
		return fmt.Errorf("inserter.Put: %w", err)
	}

	return nil
}

func main() {
	// Load environment variables from .env file.
	if err := godotenv.Load(); err != nil {
		log.Println("Warning: Could not load .env file.")
	}

	projectID := os.Getenv("PROJECT_ID")
	datasetID := os.Getenv("BIG_QUERY_DATASET_ID")
	tableID := os.Getenv("BIG_QUERY_TABLE_ID")

	if projectID == "" || datasetID == "" || tableID == "" {
		log.Fatal("Error: Ensure PROJECT_ID, BIG_QUERY_DATASET_ID, and BIG_QUERY_TABLE_ID are set.")
	}

	if projectID == "your-gcp-project-id" {
		log.Fatal("Error: Please update PROJECT_ID in your .env file.")
	}

	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		log.Fatalf("bigquery.NewClient: %v", err)
	}
	defer client.Close()

	// Optional: insert a sample row when BIG_QUERY_INSERT_SAMPLE=1
	if os.Getenv("BIG_QUERY_INSERT_SAMPLE") == "1" {
		now := time.Now().UTC()

		row := EventRow{
			EventID:   fmt.Sprintf("evt-%d", now.UnixNano()),
			DeviceID:  "device-123",
			Timestamp: now,
			Temperature: bigquery.NullFloat64{
				Float64: 27.35,
				Valid:   true, // Set to false for NULL values
			},
		}

		if err := insertEvents(ctx, client, datasetID, tableID, []EventRow{row}); err != nil {
			log.Fatalf("insertEvents failed: %v", err)
		}
		fmt.Println("Inserted 1 sample row.")
	}

	// Run the query function.
	if err := queryEventsTable(projectID, datasetID, tableID); err != nil {
		log.Fatalf("Failed to run query: %v", err)
	}
}
