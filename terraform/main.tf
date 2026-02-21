provider "google" {
  project = var.project_id
  region  = var.region
}

resource "google_bigquery_dataset" "raw" {
  dataset_id = "raw"
  location   = var.region
  description = "Сырые данные"
}

resource "google_bigquery_dataset" "dwh" {
  dataset_id = "dwh"
  location   = var.region
  description = "Витрины данных"
}

# Таблицы можно создать через ресурсы, но проще создать через dbt или руками
# Пример создания таблицы orders:
resource "google_bigquery_table" "orders" {
  dataset_id = google_bigquery_dataset.raw.dataset_id
  table_id   = "orders"
  schema = <<EOF
[
  {
    "name": "order_id",
    "type": "INTEGER",
    "mode": "REQUIRED"
  },
  {
    "name": "product_id",
    "type": "INTEGER",
    "mode": "REQUIRED"
  },
  {
    "name": "quantity",
    "type": "INTEGER"
  },
  {
    "name": "price",
    "type": "FLOAT"
  },
  {
    "name": "order_date",
    "type": "TIMESTAMP"
  }
]
EOF
}

variable "project_id" {
  description = "GCP Project ID"
  type        = string
}

variable "region" {
  description = "GCP Region"
  type        = string
  default     = "EU"
}
