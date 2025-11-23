# Entity Relationship Diagram (ERD)

## Dimensions

### dim_customer
- customer_id (PK)
- first_name
- last_name
- email
- country
- marketing_channel
- created_ts
- signup_date

### dim_product
- product_id (PK)
- product_name
- category
- brand
- current_price
- cost

### dim_date
- date_key (PK)
- date
- year
- month
- day
- week_of_year
- day_name

## Fact

### fact_order_items
- order_item_id (PK)
- order_id
- customer_id (FK → dim_customer.customer_id)
- product_id (FK → dim_product.product_id)
- order_date_key (FK → dim_date.date_key)
- order_date
- order_status
- currency
- quantity
- unit_price
- discount_amount
- extended_gross
- extended_discount
- extended_net
- cost_amount
- margin_amount
