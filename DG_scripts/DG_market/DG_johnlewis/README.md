# 🛍️ John Lewis Product Scraper

## 📌 Overview

This project is a **high-performance asynchronous scraper** for extracting product data from the **John Lewis** website via its GraphQL API and product sitemaps.
It is designed for **scalability, resilience, and clean structured data output**, making it ideal for large-scale eCommerce data collection.

The script fetches product details such as pricing, categories, attributes, descriptions, images, videos, reviews, and barcodes — then exports everything into a structured **CSV file**.

---

## ✨ Key Features

* 🚀 **Asynchronous Scraping** – built with `asyncio` + `httpx` for fast, concurrent requests.
* 📂 **Smart Caching** – reuses downloaded sitemaps/products to minimize redundant requests.
* 💾 **CSV Export** – outputs a clean, standardized CSV with 80+ fields (ready for analysis, integration, or resale).
* 🏷️ **Complete Product Coverage**:

  * Product name, SKU, brand, categories
  * Price, previous price, on-sale detection
  * UPC / EAN barcodes
  * Images (up to 5), video URLs
  * Descriptions (cleaned & formatted)
  * Attributes (up to 20 specifications)
  * Warranty info, promotional flags
  * Reviews (count & rating)
* 🛡️ **Error Handling & Retry Logic** – automatic retry with exponential backoff for failed requests.
* 🧹 **Data Cleaning** – removes HTML, bullet points, and formatting inconsistencies.

---

## 📊 Example Output

The script generates a CSV like this (sample):

| source    | date       | url                                                         | sku       | name                | brand      | price  | previousPrice | onSale | reviewCount | reviewRating | image1                            | videoURL                          | attributeTitle1 | attributeValue1     |
| --------- | ---------- | ----------------------------------------------------------- | --------- | ------------------- | ---------- | ------ | ------------- | ------ | ----------- | ------------ | --------------------------------- | --------------------------------- | --------------- | ------------------- |
| JohnLewis | 2025-09-16 | [https://www.johnlewis.com/](https://www.johnlewis.com/)... | 114260220 | Pocket Sprung Divan | John Lewis | 769.30 | 1099.00       | Y      | 134         | 4.6          | [https://media](https://media)... | [https://media](https://media)... | Dimensions      | H36 x W185 x D200cm |

---

## ⚙️ How It Works

1. **Fetch Sitemaps**

   * Downloads product sitemaps (`.xml.gz`) from John Lewis.
   * Extracts all product IDs.

2. **Fetch Product Data**

   * Queries the John Lewis **GraphQL API** for each product ID.
   * Supports both **active** and **retired** products.

3. **Extract & Clean Data**

   * Price & promotion logic
   * Images (primary + alternates)
   * Video stream URLs
   * Barcodes (UPC, EAN13, EAN8)
   * Review ratings & counts
   * Product attributes (up to 20)
   * Category hierarchy (cat → subcat5)

4. **Export to CSV**

   * Saves results to `outputs/products.csv`
   * Uses a standardized schema for easy import into Excel, BI tools, or databases.

---

## 🛠️ Tech Stack

* **Python 3.10+**
* `asyncio` – async tasks
* `httpx` – HTTP client
* `gzip`, `xml.etree.ElementTree` – sitemap parsing
* `re` & `BeautifulSoup` – text cleaning
* `csv` – structured export
* `tqdm` – progress bar

---

## 📂 Project Structure

```
├── DG_johnlewis/
│   ├── DG_johnlewis.py       # Main scraper
│   ├── functions.py          # Utility functions
│   ├── outputs/              # CSV results
│   ├── products/             # Cached product data
│   ├── sitemaps/             # Cached sitemaps
│   └── logs/                 # Log files
├── products.csv              # Example output
└── README.md                 # Documentation
```

---

## 🚀 Usage

1. **Install dependencies**

   ```bash
   pip install httpx beautifulsoup4 tqdm
   ```

2. **Run the scraper**

   ```bash
   python DG_johnlewis.py
   ```

3. **Check results**

   * CSV will be saved in: `outputs/products.csv`

---

## 💡 Why This Script Is Special

Unlike generic scrapers, this solution:

* Uses the **official GraphQL API** instead of fragile HTML parsing.
* Handles **both active and retired products**.
* Includes **advanced cleaning** of attributes, bullet points, and HTML.
* Extracts **multi-level categories** for precise classification.
* Provides **resilient error handling** and respects site limits with randomized delays.
* Is built for **scalability** (can scrape tens of thousands of products).

---

## 👨‍💻 About Me

I specialize in:

* **Custom Web Scraping** (eCommerce, real estate, directories, APIs).
* **Data Extraction & Cleaning** for business intelligence.
* **Automation Scripts** (Python + AsyncIO + Selenium/Playwright when needed).
* Delivering **scalable, production-ready solutions** with clean outputs.

This script demonstrates how I can build **robust, high-quality scrapers** tailored to client needs.

---

✅ If you’re looking for a **scraper developer who delivers clean, structured, and scalable solutions**, let’s work together