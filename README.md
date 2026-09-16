# Kong Event Gateway Demo — Multi-Region Orders

Two real Kafka problems, solved at the gateway instead of in application code:

1. **Producer → topic routing.** Regional order services publish to one logical
   topic, `orders`. They don't know — and shouldn't need to know — that orders
   physically land in `orders.us` or `orders.eu`. Kong Event Gateway resolves
   that routing transparently, based on *who* is producing (`topic_aliases`
   with a per-principal condition). Add a new region tomorrow and zero
   producer code changes.
2. **Consumer-side data skipping.** Synthetic/canary test traffic is mixed
   into the real order stream at low volume (~20% of orders, tagged
   `condition: test`), a common pattern for continuously exercising
   production infra safely. A production consumer must never process a test
   order (it would corrupt real revenue metrics), and a test/QA consumer only
   cares about test orders (no need to expose it to real customer data, and
   no need to wade through 5x the real traffic to find its own). Both
   consumers subscribe to the exact same two topics — Kong's `skip_record`
   policy on each one's virtual cluster drops the other's records before
   they ever reach that process. Neither consumer contains a line of
   filtering code, and the isolation holds even if a consumer is
   misconfigured to subscribe to the "wrong" topic.

## Architecture

```mermaid
flowchart LR
    PUS[Producer · US<br/>1 msg / 1-5s] -->|SASL: us-producer-svc| KEG
    PEU[Producer · EU<br/>1 msg / 1-5s<br/>~20% tagged condition=test] -->|SASL: eu-producer-svc| KEG

    subgraph KEG[Kong Event Gateway]
        direction LR
        VC1[virtual cluster: orders-vc<br/>alias "orders" routed by principal]
        VC2[virtual cluster: test-vc<br/>skip_record: condition != test]
        VC3[virtual cluster: prod-vc<br/>skip_record: condition != prod]
    end

    KEG -->|routed| TUS[(orders.us)]
    KEG -->|routed| TEU[(orders.eu)]

    TUS --> AN[Analytics consumer<br/>sees everything]
    TEU --> AN
    TUS --> TC[Test consumer<br/>condition=test only]
    TEU --> TC
    TUS --> PC[Prod consumer<br/>condition=prod only]
    TEU --> PC
    TUS -.->|dropped by skip_record| X[✕]
    TEU -.->|dropped by skip_record| X

    TUS -. Confluent Cloud .- TEU
```

A live version of this diagram, with messages animating across it and running
counters (produced / delivered / skipped), is served by `scripts/dashboard_server.py`.

## Backend

- Kafka: an existing Confluent Cloud cluster (bring your own — fill in `.env`).
- Event Gateway: Konnect-managed control plane + a local Docker data plane,
  provisioned with Kong's own quickstart:
  ```
  curl -Ls https://get.konghq.com/event-gateway | bash -s -- -k "$(cat ~/.kong/kpat)" -n orders-demo-event-gateway
  ```

> Note: this directory also contains `scripts/kong-cp-setup.sh`, `kong/certs/`,
> and a classic-Gateway control plane also named `event-gateway-demo` — those
> are leftovers from an unrelated demo and are not used by anything here.

## Running it

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 1. fill in .env: BOOTSTRAP_SERVERS, KAFKA_API_KEY, KAFKA_API_SECRET (Confluent Cloud)

# 2. create the two backend topics (idempotent)
python scripts/create_confluent_topics.py

# 3. configure the Event Gateway: backend cluster, listener, virtual clusters,
#    topic routing, ACLs, and both skip_record policies (idempotent)
python scripts/setup_konnect.py

# 4. start the dashboard
python scripts/dashboard_server.py &
open http://127.0.0.1:8090

# 5. start producers and consumers
python scripts/producer.py --region us &
python scripts/producer.py --region eu &
python scripts/consumer.py --persona analytics &
python scripts/consumer.py --persona test &
python scripts/consumer.py --persona prod &
```

Watch the dashboard: analytics' counter tracks every order produced; the test
consumer's counter only ever climbs on `condition=test` orders and the prod
consumer's only on `condition=prod` orders — each is exactly one skip counter
behind the other's total. "Leaked" stays at 0/0 the whole time, proving
neither consumer ever receives the other's data, even though both subscribe
to the same two topics.

## Verifying the routing independently

```bash
# both producers write to "orders" — but land on different physical topics
python -c "
from confluent_kafka.admin import AdminClient
import os, dotenv; dotenv.load_dotenv('.env')
a = AdminClient({'bootstrap.servers': os.environ['BOOTSTRAP_SERVERS'],
                 'security.protocol':'SASL_SSL','sasl.mechanisms':'PLAIN',
                 'sasl.username':os.environ['KAFKA_API_KEY'],'sasl.password':os.environ['KAFKA_API_SECRET']})
print(a.list_topics(timeout=10).topics.keys())
"
```

Or watch either topic directly in the Confluent Cloud console — `orders.us`
only ever contains `"region":"US"` records and `orders.eu` only `"region":"EU"`.
