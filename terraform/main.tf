# Terraform Configuration for NIST RT-RADP Project
# Provisions cloud-native storage and analytics infrastructure using IaC

terraform {
  required_version = ">= 1.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 5.6.0"
    }
  }
}

provider "google" {
  # Your actual Project ID: nist-anomaly-de-2026
  project     = "nist-anomaly-de-2026"
  region      = "asia-south1"
  credentials = file("creds.json")
}

# --------------------------------------------------
# 1. Data Lake (Google Cloud Storage - Raw Zone)
# --------------------------------------------------
resource "google_storage_bucket" "nist_datalake_raw" {
  # Globally unique, environment-scoped bucket name
  name          = "nist-anomaly-datalake-dev-jhakr"
  location      = "ASIA-SOUTH1"
  storage_class = "STANDARD"

  # Allows teardown during development; disable in prod
  force_destroy = true

  # Enforce private access for sensitive telemetry
  public_access_prevention    = "enforced"
  uniform_bucket_level_access = true

  # Cost-optimized lifecycle: move cold data to Nearline after 60 days
  # This keeps your 14.5 GB accessible but cheaper to store long-term
  lifecycle_rule {
    condition {
      age = 80
    }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }
}

# --------------------------------------------------
# 2. Data Warehouse (BigQuery - Analytics Zone)
# --------------------------------------------------
resource "google_bigquery_dataset" "nist_analytics" {
  dataset_id    = "nist_anomaly_analytics"
  friendly_name = "NIST Anomaly Analytics Warehouse"
  description   = "Analytics-ready RAN telemetry and anomaly detection outputs"
  location      = "ASIA-SOUTH1"

  # Table lifecycle managed explicitly (no auto-expiry)
  default_table_expiration_ms = null

  labels = {
    env     = "dev"
    domain  = "anomaly-detection"
    owner   = "jhakrishna"
  }
}