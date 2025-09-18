# 🛒 AO.com Product Scraper

## 📖 Overview
This project is a **professional Python web scraper** built to extract structured product data from [AO.com](https://ao.com).  
It is designed with **performance, reliability, and flexibility** in mind — making it ideal for **freelance projects, data pipelines, and business intelligence**.

One of the key challenges in modern scraping is **bypassing anti-bot protections** such as **Cloudflare**.  
This scraper is engineered to **reliably fetch data while handling Cloudflare challenges**, ensuring uninterrupted data collection.

---

## ✨ Features
- 🚀 **High-Performance Asynchronous Scraping**  
  Uses `asyncio` to process multiple requests in parallel, significantly speeding up data collection.

- 🛡 **Cloudflare Bypass**  
  Built-in handling to bypass **Cloudflare protection** and ensure data extraction from protected endpoints.

- 📊 **Comprehensive Product Data**  
  Extracts rich details including:
  - Product name, brand, SKU
  - Price & previous price (sale detection)
  - Category & subcategories
  - Long/short descriptions
  - Review count & rating
  - Images (up to 5 per product)
  - Specifications (dynamic extraction)

- 📝 **Structured CSV Output**  
  - Clean, standardized headers for easy integration  
  - Compatible with Excel, BI tools, and data pipelines

- 🔄 **Retry & Error Handling**  
  - Automatic retries for failed requests  
  - Logging system to capture issues and keep track of scraping sessions

- 📦 **Scalable Design**  
  - Can be extended to scrape more categories, multiple domains, or integrated into larger data pipelines

---

## 🛠 Tech Stack
- **Language:** Python 3.10+  
- **Core Libraries:**  
  - `asyncio`, `httpx`, `tqdm`  
  - `BeautifulSoup4`, `re`, `json`  
  - `csv`, `logging`  
- **Optional:** ScrapingBee API or custom headers/proxies for enhanced Cloudflare bypass  

---

## 📂 Project Structure
```

├── DG\_ao\_com.py      # Main scraper script
├── functions.py      # Helper functions (fetching, logging, configs)
├── products.csv      # Sample output file
├── logs/             # Logs of scraping sessions
├── data/             # Cached HTML, sitemaps, products
└── README.md         # Documentation

````

---

## 🚀 How to Run
1. Clone the repository:
   ```bash
   git clone https://github.com/Mostafa3mad/web_scraping.git
   cd web_scraping/DG_scripts/DG_market/DG_ao_com

2. Install dependencies:

   ```bash
   pip install -r requirements.txt
   ```

3. Run the scraper:

   ```bash
   python DG_ao_com.py
   ```

4. Output will be saved to:

   ```
   outputs/products.csv
   ```

---

## 📊 Sample Output

| source | date       | url | sku     | name               | brand | price  | reviewCount | reviewRating |
| ------ | ---------- | --- | ------- | ------------------ | ----- | ------ | ----------- | ------------ |
| ao.com | 2025-09-17 | ... | AOTeddy | Bear, The AO Teddy | AO    | £15.00 | 22          | 4.7          |

---

## 🔎 Use Cases

* 🛒 **E-commerce intelligence** → Monitor competitors’ prices, categories, and promotions
* 📊 **Data analysis** → Build structured datasets for research or BI dashboards
* 💼 **Freelance projects** → Demonstrate advanced scraping, anti-bot bypass, and data pipeline skills

---

## 👤 Author

Developed by **Mostafa EMAD**
📬 Available for custom scraping & automation solutions — perfect for **Upwork clients** needing **Cloudflare bypass + reliable data extraction**.

## 📜 License
This project is licensed under the [MIT License](./LICENSE).
