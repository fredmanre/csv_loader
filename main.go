package main

import (
	"bufio"
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/schollz/progressbar/v3"
)

// Trade represents a single trade record from the CSV
type Trade struct {
	Timestamp           time.Time
	TradeID             string
	Pair                string
	Exchange            string
	Price               float64
	Volume              *float64 // Pointer to allow NULL values
	Provider            string
	DispatcherTimestamp time.Time
}

// Config holds application configuration
type Config struct {
	DBConnectionString string
	CSVFilePath        string
	BatchSize          int
	WorkerCount        int
	TableName          string
	SchemaName         string
	ErrorLogPath       string // Path for error log file
}

// WorkerPayload represents a batch of trades to be processed by a worker
type WorkerPayload struct {
	Trades []Trade
	ID     int
}

func main() {
	// Parse command line arguments
	dbConnStr := flag.String("db", "", "Database connection string")
	csvFile := flag.String("file", "", "Path to CSV file")
	batchSize := flag.Int("batch", 15000, "Batch size for processing")
	workerCount := flag.Int("workers", runtime.NumCPU(), "Number of workers")
	schemaName := flag.String("schema", "inbound", "Schema name")
	tableName := flag.String("table", "price_trade_crypto", "Table name")
	errorLogPath := flag.String("errorlog", "", "Path to save error records (default: inputfile_errors.csv)")
	flag.Parse()

	if *csvFile == "" {
		log.Fatal("CSV file path is required")
	}

	// If no error log path is provided, use the input filename with "_errors.csv" suffix
	defaultErrorLog := ""
	if *errorLogPath == "" {
		defaultErrorLog = *csvFile + "_errors.csv"
		errorLogPath = &defaultErrorLog
	}

	config := Config{
		DBConnectionString: *dbConnStr,
		CSVFilePath:        *csvFile,
		BatchSize:          *batchSize,
		WorkerCount:        *workerCount,
		TableName:          *tableName,
		SchemaName:         *schemaName,
		ErrorLogPath:       *errorLogPath,
	}

	// Get file size for progress bar
	fileInfo, err := os.Stat(config.CSVFilePath)
	if err != nil {
		log.Fatalf("Error getting file info: %v", err)
	}
	fileSize := fileInfo.Size()

	// Open CSV file
	file, err := os.Open(config.CSVFilePath)
	if err != nil {
		log.Fatalf("Error opening file: %v", err)
	}
	defer file.Close()

	// Create progress bar
	bar := progressbar.DefaultBytes(
		fileSize,
		"Loading CSV to TimescaleDB",
	)

	// Initialize database connection pool
	pool, err := pgxpool.New(context.Background(), config.DBConnectionString)
	if err != nil {
		log.Fatalf("Unable to connect to database: %v", err)
	}
	defer pool.Close()

	// Validate connection
	if err := pool.Ping(context.Background()); err != nil {
		log.Fatalf("Unable to ping database: %v", err)
	}

	fmt.Printf("Starting import with %d workers and batch size of %d\n", config.WorkerCount, config.BatchSize)

	// Create worker pool and channel for payloads
	jobs := make(chan WorkerPayload, config.WorkerCount*2)
	var wg sync.WaitGroup

	// Start worker pool
	for i := 0; i < config.WorkerCount; i++ {
		wg.Add(1)
		go worker(pool, jobs, &wg, config, i)
	}

	// Process CSV file
	reader := csv.NewReader(bufio.NewReader(file))
	reader.ReuseRecord = true
	reader.TrimLeadingSpace = true

	// Create error log file
	errorFile, err := os.Create(config.ErrorLogPath)
	if err != nil {
		log.Fatalf("Error creating error log file: %v", err)
	}
	defer errorFile.Close()

	// Create a CSV writer for errors
	errorWriter := csv.NewWriter(errorFile)
	defer errorWriter.Flush()

	// Mutex for concurrent access to error file
	var errorMutex sync.Mutex

	var trades []Trade
	var totalRows int64
	var batchCount int
	var errorCount int

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error reading CSV record: %v", err)
			continue
		}

		// Skip header if present
		if totalRows == 0 && strings.Contains(record[0], "timestamp") {
			// Write header to error file also
			errorMutex.Lock()
			errorWriter.Write(record)
			errorWriter.Flush()
			errorMutex.Unlock()

			// Update progress bar
			bar.Add64(int64(len(strings.Join(record, ","))) + 1)
			totalRows++
			continue
		}

		// Parse trade data
		trade, err := parseTrade(record)
		if err != nil {
			log.Printf("Error parsing trade: %v for record: %v", err, record)

			// Write error record to error file
			errorMutex.Lock()
			errorWriter.Write(record)
			errorWriter.Flush()
			errorCount++
			errorMutex.Unlock()

			// Update progress bar even for skipped records
			bar.Add64(int64(len(strings.Join(record, ","))) + 1)
			continue
		}

		trades = append(trades, trade)
		totalRows++

		// Update progress bar with approximate bytes read
		bar.Add64(int64(len(strings.Join(record, ","))) + 1)

		// When we reach batch size, send to worker
		if len(trades) >= config.BatchSize {
			jobs <- WorkerPayload{
				Trades: trades,
				ID:     batchCount,
			}
			batchCount++
			trades = make([]Trade, 0, config.BatchSize)
		}
	}

	// Send any remaining trades
	if len(trades) > 0 {
		jobs <- WorkerPayload{
			Trades: trades,
			ID:     batchCount,
		}
	}

	// Close jobs channel to signal workers that no more jobs are coming
	close(jobs)

	// Wait for all workers to finish
	wg.Wait()

	// Complete progress bar
	_ = bar.Finish()

	fmt.Printf("Import completed. Processed %d rows in %d batches.\n", totalRows, batchCount+1)
	fmt.Printf("Found %d error records. Saved to: %s\n", errorCount, config.ErrorLogPath)
}

// parseTrade converts a CSV record to a Trade struct
func parseTrade(record []string) (Trade, error) {
	if len(record) < 8 {
		return Trade{}, fmt.Errorf("record has fewer than 8 fields: %v", record)
	}

	// Parse timestamp
	timestamp, err := time.Parse("2006-01-02 15:04:05.999999-07", record[0])
	if err != nil {
		return Trade{}, fmt.Errorf("invalid timestamp format: %s", record[0])
	}

	// Parse price (index 4)
	price, err := strconv.ParseFloat(record[4], 64)
	if err != nil {
		return Trade{}, fmt.Errorf("invalid price: %s", record[4])
	}

	// Parse volume (index 5) - allowing NULL values
	var volume *float64
	if record[5] == "" {
		// If the field is empty, leave volume as nil (NULL)
		volume = nil
	} else {
		// If there's a value, parse it
		v, err := strconv.ParseFloat(record[5], 64)
		if err != nil {
			return Trade{}, fmt.Errorf("invalid volume: %s", record[5])
		}
		volume = &v
	}

	// Parse dispatcher timestamp
	dispatcherTimestamp, err := time.Parse("2006-01-02 15:04:05.999999-07", record[7])
	if err != nil {
		return Trade{}, fmt.Errorf("invalid dispatcher timestamp format: %s", record[7])
	}

	// Provider is at index 6
	provider := record[6]

	return Trade{
		Timestamp:           timestamp,
		TradeID:             record[1],
		Pair:                record[2],
		Exchange:            record[3],
		Price:               price,
		Volume:              volume,
		Provider:            provider,
		DispatcherTimestamp: dispatcherTimestamp,
	}, nil
}

// worker processes batches of trades and inserts them into the database
func worker(pool *pgxpool.Pool, jobs <-chan WorkerPayload, wg *sync.WaitGroup, config Config, workerID int) {
	defer wg.Done()

	for payload := range jobs {
		startTime := time.Now()

		// Log start of batch processing
		log.Printf("Worker %d: Starting batch %d with %d trades", workerID, payload.ID, len(payload.Trades))

		// Insert batch into database
		err := insertBatch(pool, payload.Trades, config)
		if err != nil {
			log.Printf("Worker %d: Error inserting batch %d: %v", workerID, payload.ID, err)
			continue
		}

		// Log completion of batch processing
		duration := time.Since(startTime)
		log.Printf("Worker %d: Completed batch %d with %d trades in %v", workerID, payload.ID, len(payload.Trades), duration)
	}
}

// insertBatch inserts a batch of trades into the database using COPY
func insertBatch(pool *pgxpool.Pool, trades []Trade, config Config) error {
	ctx := context.Background()
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	tableName := fmt.Sprintf("%s.%s", config.SchemaName, config.TableName)
	fmt.Println(tableName)

	// Use COPY for efficient batch insertion
	copyCount, err := tx.CopyFrom(
		ctx,
		pgx.Identifier{config.SchemaName, config.TableName},
		[]string{"timestamp", "trade_id", "pair", "exchange", "price", "volume", "provider", "dispatcher_timestamp"},
		pgx.CopyFromSlice(len(trades), func(i int) ([]interface{}, error) {
			return []interface{}{
				trades[i].Timestamp,
				trades[i].TradeID,
				trades[i].Pair,
				trades[i].Exchange,
				trades[i].Price,
				trades[i].Volume, // Can be nil, which TimescaleDB will interpret as NULL
				trades[i].Provider,
				trades[i].DispatcherTimestamp,
			}, nil
		}),
	)

	if err != nil {
		return fmt.Errorf("copy command failed: %w", err)
	}

	if int(copyCount) != len(trades) {
		return fmt.Errorf("expected to copy %d rows, copied %d", len(trades), copyCount)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	return nil
}
