# tap-skubana

## Streams

- sales_order_line
    - Primary keys: id
    - Replication strategy: Incremental (query filtered)
      - Bookmark query parameter: lastModified
      - Bookmark: lastModified
    - Transformation: objects de-nested, camel to snake case, `/` to underscore, remove `$` 

- customer
    - Primary keys: id
    - Replication strategy: Incremental (query filtered)
      - Bookmark query parameter: lastModified
      - Bookmark: lastModified
    - Transformation: objects de-nested, camel to snake case, `/` to underscore, remove `$` 

- inventory
    - Primary keys: id
    - Replication strategy: Incremental (query filtered)
      - Bookmark query parameter: lastModified
      - Bookmark: lastModified
    - Transformation: objects de-nested, camel to snake case, `/` to underscore, remove `$` 

- invoice
    - Primary keys: id
    - Replication strategy: Incremental (query filtered)
      - Bookmark query parameter: lastModified
      - Bookmark: lastModified
    - Transformation: objects de-nested, camel to snake case, `/` to underscore, remove `$` 

- inventory_movement
    - Primary keys: id
    - Replication strategy: Incremental (query filtered)
      - Bookmark query parameter: lastModified
      - Bookmark: lastModified
    - Transformation: objects de-nested, camel to snake case, `/` to underscore, remove `$` 

- item
    - Primary keys: id
    - Replication strategy: Incremental (query filtered)
      - Bookmark query parameter: lastModified
      - Bookmark: lastModified
    - Transformation: objects de-nested, camel to snake case, `/` to underscore, remove `$` 

- stock_transfer
    - Primary keys: id
    - Replication strategy: Incremental (query filtered)
      - Bookmark query parameter: lastModified
      - Bookmark: lastModified
    - Transformation: objects de-nested, camel to snake case, `/` to underscore, remove `$` 

### Denesting and Transforms

Denesting promotes nested object properties to root-level. All keys converted from camel to snake case. For keys containing a `/` character, converted to "_"

Example orginal API reponse for Sales Order Line:

```json
    "charges/netAmountBeforeTax": {
        "amount": 6500,
        "currency": "USD"
    },
```

After denesting and transforms:

```json
    "charges_net_amount_before_tax_amount": 6500,
    "charges_net_amount_before_tax_currency": "USD"
```


```
Checking stdin for valid Singer-formatted data
The output is valid.
It contained 788 messages for 18 streams.

     18 schema messages
    727 record messages
     43 state messages

Details by stream:
+-------------------------+---------+---------+
| stream                  | records | schemas |
+-------------------------+---------+---------+
| application_properties  | 0       | 1       |
| company_info            | 1       | 1       |
| custom_field_definition | 0       | 1       |
| inventory               | 249     | 1       |
| listing                 | 0       | 1       |
| order                   | 71      | 1       |
| product                 | 0       | 1       |
| purchase_order          | 0       | 1       |
| rma                     | 0       | 1       |
| sales_channel           | 10      | 1       |
| shipment                | 111     | 1       |
| shipping_carrier        | 119     | 1       |
| shipping_package        | 82      | 1       |
| shipping_provider       | 0       | 1       |
| stock_transfer          | 0       | 1       |
| vendor                  | 7       | 1       |
| vendor_product          | 73      | 1       |
| warehouse               | 4       | 1       |
+-------------------------+---------+---------+
```


---

Copyright &copy; 2020 Stitch
