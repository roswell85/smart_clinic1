package main

import (
	"context"
	"fmt"
	"log"

	"github.com/segmentio/kafka-go"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func main() {
	mongoCtx := context.Background()
	client, err := mongo.Connect(mongoCtx, options.Client().ApplyURI("mongodb://admin:password@mongodb:27017"))
	if err != nil {
		log.Fatal("ERROR 1")

	}
	inventoryCol := client.Database("inventory_db").Collection("patients_folder")
	fmt.Println("🍃 Connected to MongoDB")
	fmt.Println(inventoryCol)
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"kafka:9092"},
		Topic:   "patient-events",
		GroupID: "inventory-group",
	})
	fmt.Println("🍃 Connected to Kafka")
	for {
		m, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Printf("ERROR 2: %v", err)
			break
		}
		fmt.Printf("Received message: %s\n", m.Value)
		_, er := inventoryCol.InsertOne(mongoCtx, map[string]string{
			"event_data": string(m.Value),
			"status":     "initialized",
		})

		if er != nil {
			log.Printf("Failed to save to Mongo: %v", er)
		} else {
			fmt.Println("✅ Patient folder initialized in MongoDB!")
		}

	}
}
