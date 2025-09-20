# 🛍️ Lycamobile Product Scraper

## 📌 Overview

This project is a **high-performance asynchronous scraper** for extracting product data from the **Lycamobile.co.uk** website. The scraper is designed to efficiently collect product details such as pricing, stock availability, contract details, descriptions, images, and more, and then exports everything into a clean **CSV file**.

---

## ✨ Key Features

* 🚀 **Asynchronous Scraping** – built with `asyncio` and `httpx` for fast, concurrent requests.
* 📂 **Smart Caching** – reuses downloaded sitemaps/products to minimize redundant requests.
* 💾 **CSV Export** – outputs a clean, standardized CSV with structured data (ready for analysis, integration, or resale).
* 🏷️ **Complete Product Coverage**:

  * Product name, SKU, brand, categories
  * Price, previous price, on-sale detection
  * SIM contract details and prices
  * Images (up to 5), video URLs
  * Descriptions (cleaned & formatted)
  * Attributes (up to 20 specifications)
  * Stock availability, warranty info, promotional flags
  * Reviews (count & rating)
* 🛡️ **Error Handling & Retry Logic** – automatic retry with exponential backoff for failed requests.
* 🧹 **Data Cleaning** – removes HTML, bullet points, and formatting inconsistencies.

---

## 📊 Example Output

The script generates a CSV like this (sample):

| source    | date       | url                                                         | sku       | name                | brand      | price  | previousPrice | onSale | reviewCount | reviewRating | image1                            | videoURL                          | attributeTitle1 | attributeValue1     |
| --------- | ---------- | ----------------------------------------------------------- | --------- | ------------------- | ---------- | ------ | ------------- | ------ | ----------- | ------------ | --------------------------------- | --------------------------------- | --------------- | ------------------- |
| Lycamobile | 2025-09-16 | [https://www.lycamobile.co.uk/](https://www.lycamobile.co.uk/)... | 114260220 | Pocket Sprung Divan | Lycamobile | 769.30 | 1099.00       | Y      | 134         | 4.6          | [https://media](https://media)... | [https://media](https://media)... | Dimensions      | H36 x W185 x D200cm |

---

## ⚙️ How It Works

1. **Fetch Sitemaps**

   * Downloads product sitemaps from Lycamobile.
   * Extracts all product URLs.

2. **Fetch Product Data**

   * Queries each product URL to fetch detailed information.
   * Supports retrieving data for active and discontinued products.

3. **Extract & Clean Data**

   * Price and promotion logic
   * Images (primary + alternates)
   * SIM contract details
   * Review ratings & counts
   * Product attributes (up to 20)
   * Stock availability
   * Category hierarchy and descriptions

4. **Export to CSV**

   * Saves results to `outputs/products.csv`
   * Standardized schema for easy import into Excel, BI tools, or databases.

---

## 🛠️ Tech Stack

* **Python 3.10+**
* `asyncio` – async tasks
* `httpx` – HTTP client
* `csv` – structured export
* `BeautifulSoup` – HTML text cleaning
* `tqdm` – progress bar for task tracking

---

## 📂 Project Structure

```

├── DG\_lycamobile/
│   ├── DG\_lycamobile.py       # Main scraper script
│   ├── functions.py          # Utility functions for data extraction
│   ├── outputs/              # Folder containing the CSV output
│   ├── products/             # Cached product data
│   ├── sitemaps/             # Cached sitemaps
│   └── logs/                 # Log files
├── products.csv              # Example output CSV
└── README.md                 # Project documentation

````

---

## 🚀 Usage

1. **Install dependencies**

   ```bash
   pip install httpx beautifulsoup4 tqdm
````

2. **Run the scraper**

   ```bash
   python DG_lycamobile.py
   ```

3. **Check results**

   * The CSV file will be saved in: `outputs/products.csv`

---

## 💡 Why This Script Is Special

This scraper is built to:

* Efficiently extract detailed product data from Lycamobile, including SIM contracts, stock, and reviews.
* Utilize asynchronous programming for faster and scalable scraping.
* Handle both active and discontinued products.
* Provide clean and structured data for easy analysis or integration into databases.
* Automatically retry on failure with exponential backoff, ensuring resilient scraping.

---

## 👨‍💻 About Me

I specialize in:

* **Custom Web Scraping** (eCommerce, APIs, directories).
* **Data Extraction & Cleaning** for business intelligence.
* **Automation Scripts** (Python + AsyncIO + Selenium/Playwright when needed).
* Delivering **scalable, production-ready solutions** with clean outputs.

This script demonstrates how I can build **robust, high-quality scrapers** tailored to client needs.

---

✅ If you're looking for a **scraper developer who delivers clean, structured, and scalable solutions**, let's work together!

```

---

In this file:
- The **overview** explains the purpose of the project.
- The **key features** section highlights the important aspects of the scraper.
- The **example output** shows what the resulting CSV will look like.
- The **usage section** guides the user through installation, running the script, and finding the results.
- The **Why This Script Is Special** section emphasizes the benefits of the scraper and its features.
```
