# DS_3022_DP3

## Overview

**API** -> https://openskynetwork.github.io/opensky-api/

**Aircraft Data Samples** -> https://opensky-network.org/datasets/#metadata/

**All Scientific Datasets** -> https://opensky-network.org/data/scientific#d5

**Data Tools** -> https://opensky-network.org/data/tools

### Broad Guide

poll every N seconds (listen to stream) → push records → Kafka → S3 → DuckDB → dbt.

Havent gotten to the super technical details yet but I think this is a good plan/starting point.

![diagram](image.png)

idea behind project/ goal:

analyze air traffic patterns to find anaomolies -- right now I have the folders laid out in a way that we might want to look at military aircraft movement around military bases -- maybe looking for anomalies? -- we could do anything with the aircraft data really though.