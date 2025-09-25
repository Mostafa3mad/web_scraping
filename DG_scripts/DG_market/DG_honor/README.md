# HONOR Consumer Scraper

This project is part of the **DG\_market** scraping suite.
It extracts structured product data from the official [HONOR UK website](https://www.honor.com/uk/), using asynchronous scraping and API calls.

---

## Features

* Crawls the HONOR **sitemap** to discover `/buy` product pages.
* Extracts **SKU-level details** from HONOR backend APIs:

  * Product detail info
  * SKU detail info
  * Product images (multiple resolutions, up to 5 per SKU)
* Collects:

  * Product ID, SKU, and GBOM codes
  * Product name, brand
  * Price, previous price, sale flags (`onSale`, `saleText`)
  * Colour, size, and up to 20 attributes
  * Category hierarchy (from product URL)
  * Descriptions, SEO titles
  * Reviews & ratings (via JSON-LD)
  * Up to 5 images
* Outputs a **standardized CSV file** (`products.csv`) with deduplication.

---

## Requirements

* Python 3.10+
* Install dependencies:

```bash
pip install -r requirements.txt
```

Key dependencies:

* `beautifulsoup4`
* `lxml`
* `httpx`
* `tqdm`
* `pandas` (optional, CSV manipulation)

---

## Usage

Run the scraper from repo root:

```bash
python DG_HONOR.py
```

The script will:

1. Create an output CSV (`products.csv`) in `/data/outputs/`.
2. Fetch HONOR UK sitemap (`sitemap-uk-EN.xml`).
3. Extract `/buy/` product links.
4. Call APIs to collect product data.
5. Save structured rows to CSV.

---

## CSV Output

The CSV has standardized headers:

```
source,date,apiURL,url,sku,name,brand,price,previousPrice,onSale,
saleText,colour,size,UPC,EAN,cat,subcat1,subcat2,subcat3,subcat4,subcat5,
warranty,image1,image2,image3,image4,image5,
desc,shortDesc,reviewCount,reviewRating,videoURL,
isSellingFast,isRestockingSoon,isPromotion,isOutletPrice,
lowestPriceText,lowestPriceValue,
attributeType1,attributeTitle1,attributeValue1, ...
```

Each row = **one SKU variant**.

---

## Project Structure

```
DG_market/
│
├── DG_HONOR.py          # HONOR scraper
├── functions.py         # Shared helpers (fetching, config, CSV, logging)
├── data/                # Cached sitemaps, products, categories
└── outputs/             # Final CSV & logs
```

---

## Logging

* Logs are stored in `logs/scraper.log`.
* Failed requests are retried with backoff and won’t stop the loop.

---

## Notes

* Targets the **UK site** (`siteCode=UK`).
* APIs are region-specific; change `siteCode` for other regions.
* CSV saving uses deduplication to avoid duplicates (`sku`-based).

---

## Freelance Capabilities 🚀

This project demonstrates:

* **Reverse engineering private APIs**
  Extracted hidden HONOR endpoints, SKU variations, and attributes.
* **E-commerce scraping expertise**
  Handling variants, categories, prices, discounts, images.
* **Automation-ready pipelines**
  Built with `asyncio` + `httpx` for scalable scraping.
* **Client-ready outputs**
  CSV/Excel/JSON delivery in standardized schema.
* **Adaptability**
  Framework can be extended to Amazon, eBay, Shopify, AliExpress, etc.

💼 Looking for a **scraping & automation freelancer**? This is a working proof of my skills.

---

## Hire Me 📩

**Mostafa Emad** – Freelance Developer

✅ Web scraping (APIs, JS-rendered, e-commerce)
✅ Automation scripts (data collection, bots, browser automation)
✅ Data pipelines (CSV/Excel, APIs, dashboards)

* 🌍 [GitHub Portfolio](https://github.com/Mostafa3mad)
* 💼 [Upwork Profile](https://www.upwork.com/freelancers/~0179f2b4933834b31f)
* 🔗 [LinkedIn](https://www.linkedin.com/in/mostafa--emad)
