package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"

	"github.com/go-redis/redis/v8"
	"github.com/gorilla/mux"
)

type Product struct {
	PID      string `json:"product_id" validate:"nonzero"`
	Name     string `json:"product_name" validate:"nonzero"`
	Quantity int    `json:"quantity" validate:"nonzero"`
}

var products map[int]Product
var ctx = context.Background()
var redis_client *redis.Client

func main() {
	fmt.Println("----------------------------Product quantity service started--------------------------")
	products = make(map[int]Product)
	r := mux.NewRouter()

	r.HandleFunc("/products", AddProductsQuantity).Methods("POST")
	r.HandleFunc("/products", GetProductsQuantity).Methods("GET")

	SetupRedisClient()
	http.ListenAndServe(":3001", r)
}

func AddProductsQuantity(w http.ResponseWriter, r *http.Request) {
	var product Product
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Printf("AddProductsQuantity: Error occurred while reading body: %v", err)
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("bad request"))
	}

	json.Unmarshal(body, &product)
	err = UpdateQuantity(product.PID, product.Quantity)
	if err != nil {
		w.WriteHeader(500)
	}
	w.WriteHeader(200)
}

func GetProductsQuantity(w http.ResponseWriter, r *http.Request) {
	products_list := make([]Product, 0)
	keys, values := GetProducts()

	for index, key := range keys {
		value, _ := strconv.Atoi(values[index].(string))
		products_list = append(products_list, Product{PID: key, Quantity: value})
	}

	resp, _ := json.Marshal(products_list)

	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	w.Write(resp)
}

func SetupRedisClient() {
	redis_client = redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

	_, err := redis_client.Ping(ctx).Result()
	if err != nil {
		log.Println("Error occurred while connecting to redis server")
		panic(err)
	}
	log.Println("connected to redis server")
}

func UpdateQuantity(pid string, quantity int) error {
	set, err := redis_client.IncrBy(ctx, pid, int64(quantity)).Result()
	if err != nil {
		log.Printf("error updating quantity of pid: %s\n", pid)
		return err
	} else {
		log.Printf("updated quantity of pid: %s, available: %d\n", pid, set)
	}
	return nil
}

func GetProducts() ([]string, []interface{}) {
	keys, _ := redis_client.Keys(ctx, "*").Result()
	values, _ := redis_client.MGet(ctx, keys...).Result()

	return keys, values
}
