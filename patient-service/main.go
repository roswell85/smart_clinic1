package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/segmentio/kafka-go"
)

func main() {
	// --- 1. SET UP THE SUPERVISOR (CONTEXT) ---
	// We give this entire process 3 seconds to finish or it "kills" itself.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// --- 2. CONNECT TO POSTGRES ---
	//pgConnStr := "postgres://admin:password@localhost:5432/patient_db"
	pgConnStr := "postgres://admin:password@postgres:5432/patient_db"
	pgConn, err := pgx.Connect(ctx, pgConnStr)
	if err != nil {
		log.Fatalf("❌ Database unreachable: %v", err)
	}
	defer pgConn.Close(ctx)
	fmt.Println("🐘 Connected to Postgres")

	// --- 3. SET UP KAFKA PUBLISHER ---
	writer := &kafka.Writer{
		Addr:     kafka.TCP("kafka:9092"),
		Topic:    "patient-events",
		Balancer: &kafka.LeastBytes{},
	}
	defer writer.Close()

	// --- 4. EXECUTE THE WORK ---
	patientName := "Ahmed samir"
	dentalCase := "Root Canal - Tooth 36"

	// Save to SQL
	var id int
	query := "INSERT INTO patients (name, dental_case) VALUES ($1, $2) RETURNING id"
	err = pgConn.QueryRow(ctx, query, patientName, dentalCase).Scan(&id)
	if err != nil {
		log.Fatalf("❌ SQL Insert failed: %v", err)
	}
	fmt.Printf("✅ Patient #%d saved to SQL\n", id)

	// Send to Kafka
	message := fmt.Sprintf("New Patient: %s (ID: %d)", patientName, id)
	err = writer.WriteMessages(ctx, kafka.Message{
		Value: []byte(message),
	})
	if err != nil {
		log.Fatalf("❌ Kafka broadcast failed: %v", err)
	}

	fmt.Println("🚀 Message published to 'patient-events' topic!")
}
