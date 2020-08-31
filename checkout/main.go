package main

import (
	"E-Comm/checkout/utils"
	_ "E-Comm/checkout/utils"
	"fmt"
	"log"
	"net/http"
	"strconv"

	"github.com/go-redis/redis"
	"github.com/google/uuid"
	"github.com/gorilla/mux"
)

var available_client, reserve_client, order_status_client, order_product_client, product_goroutine_client *redis.Client

func main() {
	fmt.Println("----------------------------Product checkout service started--------------------------")
	r := mux.NewRouter()

	SetupRedisClient(available_client, 0)
	SetupRedisClient(reserve_client, 1)
	SetupRedisClient(order_status_client, 2)
	SetupRedisClient(order_product_client, 3)
	SetupRedisClient(product_goroutine_client, 4)

	go ConsumePaymentEvents()
	go ConsumeProductEvents()

	r.HandleFunc("/orders/{order_id}/products/{product_id}", CancelOrder).Methods("DELETE")

	http.ListenAndServe(":3003", r)
}

func CancelOrder(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	order_id := vars["order_id"]
	// product_id := vars["product_id"]

	UpdateOrder(order_id, "cancelled")
	w.WriteHeader(200)
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

func ProductGRExists(pid string) bool {
	exists, _ := product_goroutine_client.Exists(pid).Result()
	return exists == 1
}

func SetProductGR(pid string) bool {
	product_goroutine_client.Set(pid, 1).Result()
}

func IncreaseAvailability(pid string) error {
	set, err := available_client.Incr(pid).Result()
	if err != nil {
		log.Printf("error updating available quantity of pid: %s\n", pid)
		return err
	} else {
		log.Printf("updated available quantity of pid: %s, available: %d\n", pid, set)
	}
	return nil
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

func ReduceReserved(pid string) error {
	set, err := reserve_client.Incr(pid).Result()
	if err != nil {
		log.Printf("error updating reserved quantity of pid: %s\n", pid)
		return err
	} else {
		log.Printf("updated reserved quantity of pid: %s, reserved: %d\n", pid, set)
	}
	return nil
}

func UpdateOrder(order_id uuid.UUID, status string) error {
	set, err := order_status_client.Set(order_id, status).Result()
	if err != nil {
		log.Printf("error updating status order: %s\n", pid)
		return err
	} else {
		log.Printf("order: %v updated,status: %v", order_id, status)
	}
	return nil
}

func RemoveOrder(product_id string, order_id uuid.UUID) error {
	set, err := order_product_client.LRem(product_id, 1, order_id).Result()
	if err != nil {
		log.Printf("error deleting order: %s\n", pid)
		return err
	} else {
		log.Printf("order: %v deleted", order_id)
	}
	return nil
}

func ConsumePaymentEvents() string {
	msg := utils.ConsumeEvent(utils.payment_client)
	log.Printf("msg: %v", msg)

	if len(msg) > 0 {
		order_id := "dummy" //retrieve from kafka event
		UpdateOrder(order_id, "success")
	}

	ConsumePaymentEvents()
}

func ConsumeProductEvents() string {
	for {
		msg := utils.ConsumeEvent(utils.product_client)
		log.Printf("msg: %v", msg)

		if len(msg) > 0 {
			product_id := "pid" //retrieve from kafka event

			if !ProductGRExists(product_id) {
				SetProductGR(product_id)
				go AssignOrders(product_id)
			}
		}
	}
}

func AssignOrders(product_id string) {
	order_ids, _ := order_product_client.LRange(ctx, product_id, 0, -1).Result()
	if len(order_ids) == 0 {
		return
	}
	orders_status, _ := order_status_client.MGet(ctx, order_ids...).Result()

	CheckAndUpdateStatus(product_id, order_ids, orders_status)
	AssignOrders(product_id)
}

func CheckAndUpdateStatus(product_id string, orders, status []string) {
	for index, order_id := range orders {
		if status[index] == "cancelled" || status[index] == "failed" {
			RemoveOrder(product_id, order_id)
			IncreaseAvailability(product_id)
			ReduceReserved(product_id)
		} else if status[index] == "success" {
			RemoveOrder(product_id, order_id)
			ReduceReserved(product_id)
		}
	}
}
