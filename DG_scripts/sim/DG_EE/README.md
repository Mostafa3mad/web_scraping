# 📦 ⚡ EE UK Product Data Extractor — Async GraphQL Scraper (v2)

![Python](https://img.shields.io/badge/Python-3.10+-blue)
![AsyncIO](https://img.shields.io/badge/Async-Enabled-success)
![Output](https://img.shields.io/badge/Output-CSV-green)
![License](https://img.shields.io/badge/License-MIT-lightgrey)
![Status](https://img.shields.io/badge/Status-Stable-brightgreen)

## 📚 Table of Contents
- [Overview](#-overview)
- [Features](#-features)
- [Tech Stack](#-tech-stack)
- [Main Components](#-main-components)
- [Output Schema](#-output-schema)
- [How It Works](#-how-it-works)
- [Finance Logic](#-finance-logic)
- [Example Queries](#-example-queries)
- [Usage](#-usage)
- [Performance & Limitations](#-performance--limitations)
- [License](#-license)
- [Contributing](#-contributing)
- [Support](#-support)

## 🧭 Overview
This project is a **complete data extraction and automation tool** for the [EE UK website](https://ee.co.uk).  
It retrieves and structures all available product data — including **mobiles, SIM-only deals, accessories, and marketplace items** — by directly interacting with EE’s **GraphQL endpoints**.

Unlike a normal scraper, this script **reverse-engineers EE’s frontend logic** to access internal and hidden data such as:
- Finance configuration and APR plans  
- Product variants and dimensions  
- Promotions, discounts, and previous prices  
- SIM bundle combinations and contract options  

---

## 🚀 Features
- 🔍 **Full product coverage** — extracts mobiles, SIM-only plans, accessories, and marketplace devices.  
- 🧠 **Reverse-engineered GraphQL queries** — decodes `FlexPayProductDetailsQuery`, `MarketplaceDeviceQuery`, and `SimBundleQuery`.  
- 💳 **Finance simulation** — replicates EE’s internal finance calculation logic (APR, min/max basket, loan terms, and pay-today).  
- 🧩 **Variant-aware** — handles product dimensions (color, capacity, size) dynamically.  
- 🧾 **Promotion-aware** — extracts `was` and `now` pricing, discount text, and sale flags.  
- 📊 **Structured output** — exports full CSV data with up to 20 attributes per item.  
- 💪 **Error-tolerant** — recovers from API errors and gracefully skips incomplete data.  
- ⚡ **Async architecture** — uses `asyncio` for parallel requests and faster scraping.  

---

## ⚙️ Tech Stack
- **Language:** Python 3.10+  
- **Core Libraries:**  
  - `aiohttp`, `asyncio` — for async GraphQL requests  
  - `csv`, `json`, `urllib.parse`, `BeautifulSoup`  
  - `logging`, `tqdm` — for progress tracking and error handling  

---

## 🧩 Main Components
| File | Description |
|------|--------------|
| `DG_EE_v2.py` | Main extraction engine (handles all product types & GraphQL queries) |
| `functions.py` | Helper functions (async fetch, logging, config loading) |
| `output/products.csv` | Final extracted dataset |
| `logs/scraper.log` | Runtime logging and error tracking |

---

## 📈 Output Schema
Each product entry includes:
- General info: `source`, `url`, `sku`, `name`, `brand`, `stock`
- Pricing: `previousPrice`, `lowestPriceValue`, `saleText`
- Finance: `advance`, `paymentAmount`, `phoneContractDuration`, `APR`
- SIM info: `sim_price`, `sim_data`, `simDesc`, `plan_type`
- Images: up to 5 URLs
- Attributes: up to 20 structured specification fields

Sample CSV row:
```csv
source,date,url,sku,name,brand,sim_price,previousPrice,sim_data,simDesc,...
EE,2025-11-08,https://ee.co.uk/...,Y25KIDTTD,Guided Pay as you go plan £9,EE,9,0,"2GB","500 mins | parental controls",...
````

---

## 🔄 How It Works

1. **Loads** all product URLs from EE’s public sitemaps (`sitemap-shop-hybris.xml`, `sitemap-marketplace.xml`).
2. **Determines** whether each URL belongs to a contract product or standalone device.
3. **Sends** the relevant GraphQL query (`FlexPayProductDetailsQuery` / `MarketplaceDeviceQuery` / `SimBundleQuery`).
4. **Parses & normalizes** data including variants, pricing, finance options, and promotions.
5. **Exports** results into `products.csv` encoded in `UTF-8-SIG`.

---

## 🧠 Finance Logic

The script reconstructs EE’s **financeConfiguration** system:

* Reads `promoAprCode`, `overrideMinBasketValue`, and `financeOption` arrays.
* Matches applicable terms and APRs.
* Calculates monthly payments and total finance cost:

  ```
  payment = loan * (r * (1 + r)^n) / ((1 + r)^n - 1)
  ```
* Simulates short-term (3–9 months) and long-term (12–36 months) APR plans.

---

## 🧾 Example Queries

* **SimBundleQuery**

  ```graphql
  query SimBundleQuery($bundleSeoId: String!, $simSeoId: String!) {
    simBundle(simBundleInput: { bundleSeoId: $bundleSeoId, simSeoId: $simSeoId }) {
      productPlanCombinations {
        plan { code name price { payMonthlyPrice ... } }
      }
    }
  }
  ```
* **FlexPayProductDetailsQuery**
  Used for contract devices — retrieves bundle-based plans, upfront costs, and term durations.

---

## 🧪 Usage

```bash
# Clone repo
git clone https://github.com/Mostafa3mad/web_scraping.git
cd web_scraping/DG_scripts/sim/DG_EE

# Install dependencies
pip install -r requirements.txt

# Run the main script
python DG_EE_v2.py

✅ Output saved to: /output/products.csv

```
/output/products.csv
```

---

## ⚠️ Disclaimer

This tool is for **educational and analytical purposes only**.
Always comply with **EE’s Terms of Service** when using their public APIs or data.

---

## 👨‍💻 Author

**Mostafa**  
Python Automation & Data Extraction Engineer  
🌐 [LinkedIn](https://www.linkedin.com/in/mostafa--emad?originalSubdomain=eg) | 💼 [Upwork](https://www.upwork.com/freelancers/~0179f2b4933834b31f)

---


---

## ⚡ Performance & Limitations

### 🧩 Performance
- ⏱️ **Average runtime:** ~5–8 minutes for full EE site crawl (≈ 1,000+ products).
- ⚙️ **Async parallel fetching:** Uses up to 20 concurrent requests for efficiency.
- 📊 **Average speed:** 2–3 product pages per second (depending on API response).
- 💾 **Output size:** ~10–15 MB CSV file containing all devices, SIMs, and accessories.
- 🧠 **Memory footprint:** ~200–300 MB peak during intensive parallel requests.

This setup balances **speed** and **API safety** — designed to avoid rate-limiting or server blocking while ensuring full data coverage.

---

### ⚠️ Limitations
While the script achieves high accuracy and completeness, the following caveats apply:

1. **Dynamic API Changes:**  
   EE’s GraphQL endpoints and internal field names occasionally change — periodic maintenance may be required.

2. **Finance Plan Data:**  
   Some older or retired products may not have valid finance configurations (e.g., missing APR or `promoAprCode`).

3. **Hidden / Experimental Products:**  
   Certain products under testing or upcoming releases may not appear in the sitemap or API responses.

4. **Rate Limits:**  
   Sending too many parallel requests (>25) can trigger temporary throttling. The script is tuned to avoid this.

5. **Data Encoding:**  
   CSV output is encoded in **UTF-8-SIG** to ensure Excel compatibility. Non-Latin characters may need reformatting if viewed elsewhere.

---

### ✅ Recommended Usage
- Run once per day or week for data consistency.
- Use a dedicated API proxy or delay mechanism if scraping at scale.
- Always log errors (`scraper.log`) to detect missing data or schema changes early.

---

> 🧠 *This section helps future contributors (or clients) understand performance expectations and technical constraints at a glance.*

## 🪪 License
This project is licensed under the MIT License — see the [LICENSE](LICENSE) file for details.

## 🤝 Contributing
Pull requests are welcome!  
For major changes, please open an issue first to discuss what you’d like to change.
## ⭐ Support

If this project helps you, please **star** ⭐ the repo on GitHub — it really helps!

_Last updated: November 2025_
