import json, os, sys

TENANT = "018f5f7e-7b0c-7d9a-8f14-77f42f53c851"
CLIENTS, LINES = 32, 3
PRODUCTS = CLIENTS * LINES

def product_id(i):
    return "018f0f8b-7c6d-7e31-8a4f-%012x" % i

def request_id(i):
    return "018f0f8b-7c6d-7e31-9a4f-%012x" % i

def write(path, lines):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        f.write("\n".join(json.dumps(l) for l in lines) + "\n")

# Atomic variant: a Tenant root, then products referencing it.
write("atomic/seeds/01-CreateTenant.jsonl", [
    {"idempotency_key": "atomic-tenant", "tenant_id": {"$uuid": TENANT}, "name": "Bench tenant"}
])
write("atomic/seeds/02-CreateProduct.jsonl", [
    # request_id is a uuid input, which the seeder now accepts.
    {"request_id": {"$uuid": request_id(i)}, "tenant_id": {"$uuid": TENANT},
     "product_id": {"$uuid": product_id(i)}, "sku": "BENCH-%03d" % i,
     "unit_price": {"$money": {"currency": "USD", "amount": "19.99"}},
     "stock_on_hand": 100000000}
    for i in range(PRODUCTS)
])

# Saga variant: products only, no tenant row.
write("shop/seeds/01-CreateProduct.jsonl", [
    {"idempotency_key": "saga-product-%03d" % i, "tenant_id": {"$uuid": TENANT},
     "product_id": {"$uuid": product_id(i)}, "sku": "BENCH-%03d" % i,
     "name": "Bench product %d" % i,
     "unit_price": {"$money": {"currency": "USD", "amount": "19.99"}},
     "stock_on_hand": 100000000}
    for i in range(PRODUCTS)
])
print("seeded", PRODUCTS, "products")
