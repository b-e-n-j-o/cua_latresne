RUN court circuité : 
curl -s -X POST http://127.0.0.1:8000/cua/direct \
  -H "Content-Type: application/json" \
  -d '{
    "commune": "Latresne",
    "insee": "33234",
    "parcel": "AC 0496",
    "schema_whitelist": ["public"],
    "values_limit": 100,
    "carve_enclaves": true,
    "enclave_buffer_m": 120,
    "make_report": true,
    "make_map": true
  }' | jq .
