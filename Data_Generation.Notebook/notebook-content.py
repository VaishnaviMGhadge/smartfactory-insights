# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "9ca57d54-97fc-4189-99b0-00e0c5b5b9ec",
# META       "default_lakehouse_name": "lh_smartfactory",
# META       "default_lakehouse_workspace_id": "24ecf2c6-a5b3-4be9-b639-180dd34a6f27",
# META       "known_lakehouses": [
# META         {
# META           "id": "9ca57d54-97fc-4189-99b0-00e0c5b5b9ec"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

import os, random
import numpy as np

SEED = 20261115
random.seed(SEED)
np.random.seed(SEED)

base = "/lakehouse/default/Files"
folders = [
    "landing/erp", "landing/finance", "landing/budget",
    "landing/mes/in", "landing/mes/de", "landing/mes/us", "control",
]
for f in folders:
    os.makedirs(f"{base}/{f}", exist_ok=True)

print("Folders ready:", sorted(os.listdir(f"{base}/landing")))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import os
BASE = "/lakehouse/default/Files"
os.makedirs(f"{BASE}/config", exist_ok=True)
os.makedirs(f"{BASE}/generator_state", exist_ok=True)

config_yaml = """
seed: 20261115
history_start: "2023-04-01"
history_end: "2026-10-31"
fiscal_year_start_month: 4

plants:
  - {code: PUN, name: Pune, country: IN, region: India, currency: INR, timezone: Asia/Kolkata, changeover_as_downtime: true}
  - {code: CHE, name: Chennai, country: IN, region: India, currency: INR, timezone: Asia/Kolkata, changeover_as_downtime: false}
  - {code: PIT, name: Pithampur, country: IN, region: India, currency: INR, timezone: Asia/Kolkata, changeover_as_downtime: false}
  - {code: GER, name: Stuttgart, country: DE, region: EMEA, currency: EUR, timezone: Europe/Berlin, changeover_as_downtime: false}
  - {code: USA, name: Columbus, country: US, region: Americas, currency: USD, timezone: America/New_York, changeover_as_downtime: true}

machines_per_plant: {PUN: 14, CHE: 10, PIT: 8, GER: 10, USA: 8}
machines_per_line: 4

volumes:
  products: 300
  customers: 2000
  suppliers: 120
  order_lines_per_working_day: 4000

mess_level: 1
"""
with open(f"{BASE}/config/config.yaml", "w") as f:
    f.write(config_yaml)
print("config written")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import math, yaml
import numpy as np
import pandas as pd

cfg = yaml.safe_load(open(f"{BASE}/config/config.yaml"))
rng = np.random.default_rng(cfg["seed"] + 1)   # masters get their own derived seed

plants = pd.DataFrame(cfg["plants"])

machine_types = ["CNC machining centre", "Hydraulic press", "Welding cell",
                 "Assembly station", "Heat treatment furnace", "Test bench"]
paper_logging_lines = {"PUN-L1", "PUN-L2"}   # two older Pune lines log downtime on paper

rows = []
for p in cfg["plants"]:
    n = cfg["machines_per_plant"][p["code"]]
    n_lines = math.ceil(n / cfg["machines_per_line"])
    groups = np.array_split(np.arange(n), n_lines)
    for li, grp in enumerate(groups, start=1):
        line_id = f'{p["code"]}-L{li}'
        for mi in range(1, len(grp) + 1):
            older = p["code"] == "PUN"
            rows.append({
                "machine_id": f"{line_id}-M{mi:02d}",
                "line_id": line_id,
                "plant_code": p["code"],
                "machine_type": str(rng.choice(machine_types)),
                "install_year": int(rng.integers(2008, 2019) if older else rng.integers(2012, 2023)),
                "manual_downtime_logging": line_id in paper_logging_lines,
            })

machines = pd.DataFrame(rows)
lines = (machines.groupby(["line_id", "plant_code"])
         .agg(machines=("machine_id", "count")).reset_index())

plants.to_parquet(f"{BASE}/generator_state/plants.parquet", index=False)
lines.to_parquet(f"{BASE}/generator_state/lines.parquet", index=False)
machines.to_parquet(f"{BASE}/generator_state/machines.parquet", index=False)

summary = (machines.groupby("plant_code")
           .agg(machines=("machine_id", "count"), lines=("line_id", "nunique")))
print(summary)
print("Total machines:", len(machines), "| Total lines:", len(lines))
machines.head(8)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
