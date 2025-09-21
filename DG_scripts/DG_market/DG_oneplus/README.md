# DG\_oneplus Scraper

## 📌 Overview

The **DG\_oneplus scraper** is an asynchronous Python project designed to extract product information from the official **OnePlus Store**.
It collects data such as product name, price, SKU, brand, categories, images, description, specifications, and stores them in a structured **CSV file**.

This project is part of the **`web_scraping/DG_scripts/DG_market`** collection.

---

## 📂 Project Structure

```
DG_oneplus/
├── DG_oneplus.py        # Main scraper script
├── functions.py         # Utility functions (fetching, logging, saving, models)
├── products.csv         # Sample output file with scraped data
├── data/                # Local cache (sitemaps, categories, products)
├── outputs/             # Final CSV and text outputs
└── README.md            # Project documentation
```

---

## ✨ Features

* Asynchronous scraping using **asyncio + httpx**
* Extracts detailed product information:

  * **Core Info**: SKU, name, brand, price, discounts
  * **Categories**: up to 5 levels (`cat`, `subcat1` … `subcat5`)
  * **Images**: up to 5 per product
  * **Descriptions**: cleaned HTML & short description
  * **Specifications/Attributes**: up to 20 per product
* Handles **promotions & discounts** (`onSale`, `saleText`, `lowestPrice`)
* Deduplicates SKUs in CSV output
* Logs scraping progress in `logs/scraper.log`
* Optional **ScrapingBee API** integration for proxy scraping

---

## ⚙️ Requirements

* **Python 3.8+**
* Dependencies:

  * `httpx`
  * `tqdm`
  * `beautifulsoup4`
  * `lxml`

Install them with:

```bash
pip install -r requirements.txt
```

---

## 🚀 Usage

1. Clone the repository:

   ```bash
   git clone https://github.com/Mostafa3mad/web_scraping.git
   cd web_scraping/DG_scripts/DG_market/DG_oneplus
   ```

2. Create and activate a virtual environment:

   ```bash
   python -m venv venv
   source venv/bin/activate    # Linux/Mac
   venv\Scripts\activate       # Windows
   ```

3. Install dependencies:

   ```bash
   pip install -r requirements.txt
   ```

4. Run the scraper:

   ```bash
   python DG_oneplus.py
   ```

5. Output will be saved as:

   * **CSV file**: `outputs/products.csv`
   * **Optional text output**: `outputs/output_YYYY_MM_DD.txt`

---

## 📝 CSV Output Format

The output file follows a **standard schema** with fields such as:

* **Core Info**: `source, date, apiURL, url, sku, name, brand, price, previousPrice, onSale, saleText`
* **Category hierarchy**: `cat, subcat1, subcat2, subcat3, subcat4, subcat5`
* **Images**: `image1` … `image5`
* **Descriptions**: `desc, shortDesc`
* **Reviews**: `reviewCount, reviewRating`
* **Specifications**: `attributeType1..20, attributeTitle1..20, attributeValue1..20`

---

## 📊 Example Record

```csv
source,date,apiURL,url,sku,name,brand,price,previousPrice,onSale,saleText,cat,subcat1,image1,desc,...
oneplus.com,2025-09-21,https://mallapi...,https://www.oneplus.com/product-url,OP12,OnePlus 12,OnePlus,699,799,Y,"12% off",Phones,Smartphones,https://img.oneplus.com/123.jpg,"Flagship smartphone..."
```

---

## 🛠 Configuration

Default settings are defined in `functions.py` under `DEFAULT_CONFIG`:

```python
DEFAULT_CONFIG = {
    "save_raw_sitemaps": True,
    "save_raw_categories": True,
    "save_raw_products": True,
    "save_local": True,
    "use_scrapingbee": False,
    "scrapingbee_key": "",
    "scrape_products_only": False,
    "stream_output": True,
    "workers": 3,
    "min_delay": 1.0,
    "max_delay": 3.0,
    "max_retries": 1,
    "source_name": "oneplus"
}
```

You can:

* Enable `use_scrapingbee` and provide your API key for proxy scraping
* Adjust `workers` to control concurrency
* Tune delays & retries to avoid rate limiting

---

## 💡 Strengths of the Code

* **Asynchronous design**: Uses `asyncio` and `httpx` to handle multiple requests efficiently, reducing runtime.
* **Robust error handling**: Implements retries, logging, and fallback mechanisms when requests fail.
* **Dynamic parsing**: Extracts categories, attributes, prices, and images directly from OnePlus’s APIs and embedded JSON/HTML.
* **Scalable output schema**: Supports up to 20 attributes per product, ensuring flexibility for various product types.
* **Data cleaning**: Cleans descriptions and removes unwanted HTML tags for better readability.
* **Deduplication**: Prevents duplicate SKUs in the final dataset by updating existing records instead of duplicating them.
* **Progress tracking**: Uses `tqdm` to show real-time progress when processing large product lists.
* **Extensible architecture**: Utility functions (`functions.py`) are modular and can be reused for other scraping projects.
* **Logging system**: Saves detailed logs (`logs/scraper.log`) for debugging and monitoring scraping sessions.
* **Respects rate limits**: Implements configurable delays and a limited number of workers to reduce the risk of blocking.

---

## 📌 Notes

* The scraper relies on OnePlus API endpoints (JSON) and HTML parsing.
* If OnePlus changes their website or API structure, adjustments will be needed.
* Recommended to **respect site’s rate limits** and use scraping responsibly.

