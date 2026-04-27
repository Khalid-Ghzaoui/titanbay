-- Run this once to set up the raw dataset in BigQuery before loading source tables

CREATE SCHEMA IF NOT EXISTS `titanbay-494310.titanbay_raw`
OPTIONS (
  location = 'EU'
);