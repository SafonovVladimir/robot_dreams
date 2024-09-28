schema = {
    "doc": "Purchase information",
    "name": "Sales",
    "namespace": "test",
    "type": "record",
    "fields": [
        {
            "name": "client",
            "doc": "name of client",
            "type": "string"
        },
        {
            "name": "purchase_date",
            "type": "string"
        },
        {
            "name": "product",
            "type": "string"
        },
        {
            "name": "price",
            "type": "double"
        },
    ],
}