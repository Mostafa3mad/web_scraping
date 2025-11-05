# harrods-api-scraper
Async API-based product extractor for Harrods, designed to fetch full structured catalog data (pricing, variants, images, stock, attributes, categories, etc.) without relying on HTML scraping.

---

## 🚀 Overview

`harrods-api-scraper` is a fully rewritten and upgraded version of the old HTML-scraping tool.  
Instead of parsing the front-end pages, this version communicates directly with Harrods’ internal product API, making it:

- ✅ **Faster** — no browser rendering, no HTML parsing  
- ✅ **More stable** — unaffected by UI or HTML changes  
- ✅ **More complete** — extracts all product metadata (not just visible UI fields)  
- ✅ **Scalable** — async requests, batch mode, clean architecture  
- ✅ **Easier to maintain** — structured code, no fragile selectors

The script supports unlimited product extraction by ReferenceKey (SKU), with CSV export for downstream processing.

---

## 📦 Features

| Feature | Supported |
|---------|-----------|
| Async API requests | ✅  
| Full product data (name, brand, price…) | ✅  
| Variants & stock per variant | ✅  
| Dynamic pricing + sale detection | ✅  
| Extracts images, descriptions, attributes | ✅  
| Category tree mapping (cat, subcat1,…) | ✅  
| CSV export | ✅  
| Auto-cleaning of empty / useless attributes | ✅  
| Smart “in stock / out of stock” detection | ✅  
| Rounds previous price correctly for discount math | ✅  
| Handles pagination & large product lists | ✅  
| Logs + error handling | ✅  

---

## 🛠️ Requirements

```bash
python 3.10+
aiohttp
asyncio
pandas
````

Install dependencies:

```bash
pip install -r requirements.txt
```

(If you're using the standalone script version, everything is imported at runtime.)

---

## ▶️ Usage

### 1. Set your product reference keys

Example `input.txt`:

```
000000000007128127
000000000006662243
...
```

### 2. Run the scraper

```bash
python DG_harrods.py
```

The script will:

✅ Fetch all data from Harrods API
✅ Parse, normalize, and clean product fields
✅ Export final results as `products.csv`

---

## 📂 Output: CSV Columns

Below is the standard output schema the scraper generates:

| Column                            | Description                         |
| --------------------------------- | ----------------------------------- |
| source                            | Always `harrods`                    |
| date                              | Extraction date                     |
| apiURL                            | API request URL                     |
| url                               | Public product URL                  |
| sku                               | Reference Key                       |
| name                              | Product name                        |
| brand                             | Brand label                         |
| price                             | Current price                       |
| previousPrice                     | Original price before sale (if any) |
| onSale                            | Y/N                                 |
| saleText                          | e.g. `30% OFF`                      |
| stock                             | Y/N                                 |
| colour                            | Harrods colour label                |
| size                              | Variant size (if any)               |
| UPC / EAN                         | If available                        |
| cat → subcat5                     | Auto-parsed category tree           |
| desc                              | Full product description            |
| shortDesc                         | Brief quote/summary                 |
| image1..image5                    | Main image URLs                     |
| attributeTitleX / attributeValueX | Structured product attributes       |

---

## 📝 CSV Sample

```
source,date,apiURL,url,sku,name,brand,price,stock,onSale,colour,size,cat,subcat1,subcat2
harrods,2025-11-05,https://www.harrods.com/api/rpc/get...,https://www.harrods.com/en-gb/p/la-mer...,000000000007128127,The Treatment Lotion (150ml),La Mer,165.0,Y,,NO COLOUR,OS,Storefront,Beauty,Skincare
```

---

## ⚠️ Error Handling

* Retries on network failures
* Skips invalid or missing SKUs
* Logs API errors instead of stopping execution
* Auto-throttles if API rate-limits

---

## 🔮 Future Enhancements

* ✅ Add UPC/EAN extraction if Harrods exposes it in a secondary endpoint
* ✅ Optional JSON export in addition to CSV
* ✅ Support live stock polling scheduler
* ✅ CLI flags (e.g. `--threads`, `--csv`, `--json`)
* ✅ Add unit tests + CI pipeline
* ✅ Add async batching for >10k products

---

## 📄 License

MIT — free for commercial & private use.

---

## 👨‍💻 Author

Developed by **Mostafa Emad**
Rewritten from scratch with a focus on performance, data quality, and long-term stability.

For issues or contributions:
`https://github.com/Mostafa3mad/web_scraping/blob/master/DG_scripts/DG_market/DG-harrods/DG_harrods.py`


