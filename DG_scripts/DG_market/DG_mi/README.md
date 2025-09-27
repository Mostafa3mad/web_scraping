# Xiaomi UK Product Scraper

A scraper for fetching product data from the **Xiaomi UK Store** (mobiles, tablets, etc.) and exporting it into a clean CSV file.

---

## 📂 Project Structure

### 1. `DG_mi.py`
- The main script to run the scraper.
- Core functions:
  - `extract_links()`: Collects all product links from the API.
  - `fetch_single_product(id_url)`: Fetches detailed product data (JSON + specs page).
  - `extact_data_from_product_url()`: Iterates over products and calls fetch logic.
  - `main()`: Orchestrates CSV creation and scraping process.

### 2. `functions.py`
- Contains:
  - Project configuration (e.g., `DEFAULT_CONFIG`, data folders).
  - Logging setup.
  - Utility functions for HTTP requests with `httpx` (with retries & caching).
  - CSV utilities (save, append, deduplicate).

### 3. `products.csv`
- Output file containing all products.
- Key columns:
  - `sku`, `name`, `brand`, `price`, `previousPrice`, `onSale`, `saleText`
  - `colour`, `size`, `cat`, `subcat1..5`
  - `desc`, `reviewCount`, `reviewRating`
  - Images (`image1..image5`)
  - Specification fields (`attributeTitleX`, `attributeValueX`, `attributeTypeX`)

---

## ⚙️ How to Run

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
   (main libraries: `httpx`, `beautifulsoup4`, `tqdm`)

2. Run the scraper:
   ```bash
   python DG_mi.py
   ```

3. After execution, the CSV file will be generated at:
   ```
   data/outputs/products.csv
   ```

---

## 🛠️ Stock Status Logic

- Each product from the API includes the `is_out_of_stock` field.
- Logic:
  - If `is_out_of_stock == True` → `isRestockingSoon = "Y"`
  - If `is_out_of_stock == False` → product is available.

You may also add columns like:
- `inStock` = "Y" if available  
- `outOfStock` = "Y" if not available  

---

## 📜 Notes
- The product description (`row["desc"]`) is cleaned to ensure it's always a single line.
- Specifications are extracted as key-value pairs (title + value), cleaned from extra symbols (e.g., `*`).
- Up to 20 attributes per product are supported.

---

## 🚀 Future Improvements
- Add support for other Xiaomi markets (not only UK).
- Enhance image handling (download & store locally).
- Improve review collection and processing.
