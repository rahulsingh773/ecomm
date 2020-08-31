Quantity Service:
Responsible only to add or return quantity of a product.

Cart Service:
1. adds list of products to a cart for a customer (has to be persisted into DB)
2. triggers payment request on checkout, updates the available and reserved products count. 
3. listens to product status events, notify user with the status of checkout request

checkout/order-assignment service:
1. listens to payment events
2. monitors the orders and payments, assigns the order to user if payment is successful
3. publish a kafka event for product status


Workflow:
1. user adds the products to cart.
2. user sends checkout request, the request comes to cart service.
3. cart service checks in redis (the available count) if the product is available or not.
4. if product is neither available nor reserved, it returns the appropriate error message

5. if the product is not available but reserved, it waits for 10 seconds to check if product becomes available due to cancellation, payment failures.

6. if product is available, an order is created in redis with progressing state.
7. the checkout service keeps checking all the orders status, status might get changed to success/failure due to payment events.
8. order cancellation can be handelled in checkout service.
9. if the order is successful or failed, it's removed from redis and can be persisted in sql db. a kafka event is published for the product status.
10. it also checks for time of creation, if time is more than 2 minutes, it cancels the order and any successful payment prior to that gets refunded.