import pandas as pd
import httpx
from io import BytesIO
from tqdm import tqdm
from user_agent import generate_user_agent

# Configuration from your colleague's requirements
DIMENSIONS = {"SEX", "AGE", "GEO", "EDU", "NOC"}
BASE_API = "https://rplumber.ilo.org"

class ILOStatWebServiceClient:
    def __init__(self):
        # The server often rejects requests without a proper User-Agent
        self.headers = {
            "User-Agent": generate_user_agent(os='linux',device_type='desktop'),
            "Accept": "text/csv,application/parquet,*/*"
        }
        self.client = httpx.Client(timeout=60.0, follow_redirects=True, headers=self.headers)


    def get_indicator_toc(self):
        """Step 1: Discovery via /metadata/toc/indicator"""
        url = f"{BASE_API}/metadata/toc/indicator?format=.csv&lang=en"
        print(url)
        resp = self.client.get(url)
        resp.raise_for_status()

        return pd.read_csv(BytesIO(resp.content))

    def get_data_2025(self, indicator_id):
        """Step 2: Retrieval via /data/indicator using PARQUET"""
        # Using .parquet is faster and avoids the SIGSEGV of .rds
        url = f"{BASE_API}/data/indicator"
        params = {
            "id": indicator_id,
            "time": "2025",
            "format": ".parquet",
            "type": "both" # Pulls both Code and Label in one go
        }
        try:
            resp = self.client.get(url, params=params)
            if resp.status_code == 200:
                # Use BytesIO to read the parquet stream
                return pd.read_parquet(BytesIO(resp.content))
        except Exception:
            return None
        return None

# --- Main ETL Flow ---
client = ILOStatWebServiceClient()

# 1. Fetch TOC and Filter
df_toc = client.get_indicator_toc()
print(df_toc)
# # Apply the original mask logic:
# # Splits the code and ensures all mid-tokens are within your DIMENSIONS set
# def mask_logic(code):
#     parts = code.split("_")
#     # Your colleague's slice logic (dropping prefix and unit/freq)
#     core_dims = parts[2:-1] if len(parts) > 3 else parts
#     return not (set(core_dims) - DIMENSIONS)
#
# target_indicators = df_toc[df_toc['id'].apply(mask_logic)]['id'].unique()
#
# print(f"Found {len(target_indicators)} matching indicators. Starting download...")
#
# # 2. Sequential Retrieval with Checkpointing
# all_data = []
# for indicator in tqdm(target_indicators):
#     df = client.get_data_2025(indicator)
#     if df is not None and not df.empty:
#         all_data.append(df)
#
# # 3. Final Consolidation
# if all_data:
#     final_df = pd.concat(all_data, ignore_index=True)
#     print(f"ETL Complete. Rows retrieved: {len(final_df)}")
# else:
#     print("No data found for 2025 matching those criteria.")