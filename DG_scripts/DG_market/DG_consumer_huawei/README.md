Perfect 🙌 I’ll give you the **full README.md** now, with everything included:

---

# Huawei Consumer Scraper

This project is part of the **DG\_market** scraping suite.
It extracts product data from the official [Huawei Consumer UK website](https://consumer.huawei.com/uk/), using asynchronous scraping and API calls.

---

## Features

* Crawls the Huawei sitemap to discover product pages.
* Extracts **product details** via Huawei’s backend APIs:

  * **Product detail info**

    ```
    https://itrinity-de.c.huawei.com/convert/queryPrdDisplayDetailInfo
    ```
  * **PCP SKU detail info**

    ```
    https://itrinity-de.c.huawei.com/convert/v1/product-pcp/info/query
    ```
  * **Product images by SKU**

    ```
    https://itrinity-de.c.huawei.com/convert/v1/product-pcp/images/query
    ```
* Collects:

  * Product ID and SKU codes
  * Name and brand
  * Price and previous price (detects discounts)
  * Sale flags (`onSale`, `saleText`)
  * Colour, size, and up to 20 attributes
  * Category hierarchy from breadcrumbs
  * Up to 5 product images
  * Descriptions and metadata
* Saves all data into a **CSV file** (`products.csv`) with a standard header.

---

## Requirements

* Python 3.10+
* Install dependencies:

  ```bash
  pip install -r requirements.txt
  ```

Dependencies include:

* `beautifulsoup4`
* `lxml`
* `tqdm`
* `aiohttp` (for async requests)
* `pandas` (optional, for CSV manipulation)

---

## Usage

Run the scraper from the repo root:

```bash
python DG_scripts/DG_market/DG_consumer_huawei.py
```

The script will:

1. Create an output CSV (`products.csv`).
2. Fetch the Huawei UK sitemap.
3. Extract product pages containing `/buy`.
4. Call Huawei APIs to gather detailed product info.
5. Append structured rows to the CSV.

---

## CSV Output

The CSV contains standardized headers such as:

```
source,date,apiURL,url,sku,name,brand,price,previousPrice,onSale,
saleText,colour,size,UPC,EAN,cat,subcat1,subcat2,subcat3,subcat4,subcat5,
warranty,image1,image2,image3,image4,image5,
desc,shortDesc,reviewCount,reviewRating,videoURL,
isSellingFast,isRestockingSoon,isPromotion,isOutletPrice,
lowestPriceText,lowestPriceValue,
attributeType1,attributeTitle1,attributeValue1, ...
```

Each row corresponds to one **SKU variant**.

---

## Project Structure

```
DG_scripts/
│
├── DG_market/
│   ├── DG_consumer_huawei.py   # Huawei scraper
│   ├── functions.py            # Shared helpers (fetching, logging, config)
│   └── outputs/                # CSV output directory
```

---

## Logging

* Logs are saved under `logs/scraper.log`.
* Failed requests are reported but won’t stop the main loop.

---

## Notes

* The scraper targets the **UK site** (`siteCode=UK`).
* API endpoints are region-specific; for other regions, adjust the `siteCode`.
* The script currently saves CSV **locally** if `DEFAULT_CONFIG["save_local"] = True`.

---

## Freelance Work & Capabilities 🚀

This project also showcases my ability to:

* **Reverse engineer private APIs**

  * Identified Huawei’s hidden endpoints and parsed JSON responses to extract SKU-level data.

* **E-commerce scraping**

  * Extract product data from complex, JavaScript-heavy sites.
  * Handle variations (colours, sizes, bundles) with full attribute mapping.
  * Collect images, categories, and promotions.

* **Data delivery formats**

  * Export to **CSV/Excel/JSON** in a client’s custom schema.
  * Flexible headers: add/remove fields as required (e.g. `onSale`, `attributeTypeN`).

* **Automation-ready code**

  * Built with `asyncio` + `aiohttp` for performance.
  * Can be easily scheduled (cron jobs / Airflow / cloud functions).

* **Custom projects**

  * Adaptable to **any e-commerce site** (Amazon, eBay, AliExpress, Shopify stores, etc.).
  * Skilled in **web scraping + API integrations + automation bots**.

💼 If you’re looking for a **freelance developer** who can handle **scraping, automation, and API integration**, this project is a live example of my skills.

---

## Hire Me 📩

I am available for **freelance projects**:

* ✅ Web scraping (APIs, JS-rendered sites, e-commerce, SaaS platforms)
* ✅ Automation scripts (data collection, browser automation, bots)
* ✅ Data pipelines (CSV/Excel, APIs, dashboards)

📧 Contact: **Mostafa Emad**

* 🌍 [GitHub Portfolio](https://github.com/Mostafa3mad)
* 💼 [Upwork Profile](https://www.upwork.com/freelancers/~0179f2b4933834b31f)
* 🔗 [LinkedIn](https://www.linkedin.com/in/mostafa--emad)
