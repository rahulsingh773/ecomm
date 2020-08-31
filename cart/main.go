package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"time"

	"E-Comm/cart/utils"

	"github.com/go-redis/redis"
	"github.com/google/uuid"
	"github.com/gorilla/mux"
)

type kafka_event struct {
	PID          string    `json:"product_id" validate:"nonzero"`
	UserID       string    `json:"user_id" validate:"nonzero"`
	OrderID      uuid.UUID `json:"order_id" validate:"nonzero"`
	Status       string    `json:"status" validate:"nonzero"`
	Date_Created string    `json:"date_created" validate:"nonzero"`
}

var ctx = context.Background()
var available_client, reserve_client, order_status_client, order_product_client *redis.Client

func main() {
	fmt.Println("----------------------------Product cart service started--------------------------")
	r := mux.NewRouter()

	r.HandleFunc("/users/{user_id}/products/{product_id}", AddProductToCart).Methods("POST")
	r.HandleFunc("/users/{user_id}/checkout/{product_id}", Checkout).Methods("POST")

	SetupRedisClient(available_client, 0)
	SetupRedisClient(reserve_client, 1)
	SetupRedisClient(order_status_client, 2)
	SetupRedisClient(order_product_client, 3)
	http.ListenAndServe(":3002", r)
}

func AddProductToCart(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(200)
}

func Checkout(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	user_id := vars["user_id"]
	product_id := vars["product_id"]

	available := GetProductAvailability(product_id)
	reserved := GetProductReservedCount(product_id)

	if available == 0 && reserved == 0 {
		w.WriteHeader(404)
		w.Write([]byte("product not available"))
		return
	}

	if available == 0 && reserved > 0 {
		time.Sleep(10 * time.Second) //wait for 10 seconds to check if any product is available due to payment failure/cancellations
		available = GetProductAvailability(product_id)

		if available == 0 {
			w.WriteHeader(404)
			w.Write([]byte("product not available"))
			return
		}
	}
	err1 := ReduceAvailability(product_id)
	err2 := IncreaseReserved(product_id)

	if err1 != nil || err2 != nil {
		w.WriteHeader(500)
		return
	}

	order_id := uuid.New()
	event := kafka_event{PID: product_id, UserID: user_id, OrderID: order_id, Status: "progressing"}
	event_resp, _ := json.Marshal(event)
	InitializeOrder(order_id)
	utils.PublishKafkaEvent(string(event_resp))

	w.Write(200)
}

func SetupRedisClient(client *redis.Client, db int) {
	client = redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       db,
	})

	_, err := client.Ping().Result()
	if err != nil {
		log.Println("Error occurred while connecting to redis server")
		panic(err)
	}
	log.Printf("connected to redis server db: %d", db)
}

func GetProductAvailability(pid string) int {
	set, _ := available_client.Get(pid).Result()
	qty, _ := strconv.Atoi(set)

	return qty
}

func GetProductReservedCount(pid string) int {
	set, _ := reserve_client.Get(pid).Result()
	qty, _ := strconv.Atoi(set)

	return qty
}

func ReduceAvailability(pid string) error {
	set, err := available_client.Decr(pid).Result()
	if err != nil {
		log.Printf("error updating available quantity of pid: %s\n", pid)
		return err
	} else {
		log.Printf("updated available quantity of pid: %s, available: %d\n", pid, set)
	}
	return nil
}

func IncreaseReserved(pid string) error {
	set, err := reserve_client.Incr(pid).Result()
	if err != nil {
		log.Printf("error updating reserved quantity of pid: %s\n", pid)
		return err
	} else {
		log.Printf("updated reserved quantity of pid: %s, reserved: %d\n", pid, set)
	}
	return nil
}

func InitializeOrder(order_id uuid.UUID) {
	set, err := order_status_client.Set(order_id, "progressing").Result()
	if err != nil {
		log.Printf("error initializing order: %s\n", pid)
		return err
	} else {
		log.Printf("order: %v initialized", order_id)
	}

	set, err := order_product_client.RPush(product_id, order_id).Result()
	if err != nil {
		log.Printf("error appending order_id order: %s\n", pid)
		return err
	} else {
		log.Printf("order: %v initialized", order_id)
	}
	return nil
}
