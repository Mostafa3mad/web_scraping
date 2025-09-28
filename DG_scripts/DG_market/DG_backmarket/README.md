# 📘 BackMarket Scraper Documentation

## 🔎 Overview

This project is a **reverse-engineered data extraction tool** for [BackMarket](https://www.backmarket.co.uk).
Since BackMarket relies heavily on **client-side rendering**, hidden APIs, and **Cloudflare protection**, standard scraping approaches fail.
To overcome these challenges, I combined **ScrapingBee**, **reverse engineering**, and **HTML/JS parsing**.

---

## 🚀 Features

* ✅ **Cloudflare Bypass** using [ScrapingBee](https://www.scrapingbee.com) → automatic JS rendering & cookie management.
* ✅ **Reverse-engineered APIs** directly from BackMarket’s obfuscated JS bundles.
* ✅ **Product Data Extraction**: Title, brand, price, sale, description, categories, images.
* ✅ **Reviews & Ratings**: Pull reviews list + summary + rating distribution.
* ✅ **Technical Specifications**: Extract structured attributes (color, memory, processor, etc.).
* ✅ **SEO & Metadata**: Capture OpenGraph/Twitter meta tags for product pages.
* ✅ **Fallback HTML Parsing**: When API didn’t expose data, extracted from embedded `<script>` tags (`__NUXT_DATA__` & JSON-LD).
* ✅ **CSV Export**: Normalized data saved into `products.csv`.

---

## 📡 Key Endpoints (Reverse Engineered)

* **Product details**

  ```
  GET /product-page/products/:productId
  ```
* **Variants / Pickers**

  ```
  GET /product-page/products/:productId/pickers
  ```
* **Reviews (list)**

  ```
  GET /reviews/v1/products/:uuid/reviews?order_by=-relevance_alt&page_size=20&translation_locale=en-gb
  ```
* **Reviews (summary)**

  ```
  GET /reviews/products/:uuid/reviews-summary
  ```
* **Technical Specifications**

  ```
  GET /bm/product/:productId/technical_specifications
  ```

> Note: These APIs were **not visible in the browser Network tab**, but discovered inside **minified JavaScript files** and HTML scripts.

---

## ⚙️ Tech Stack

* **Python 3.10+**
* `requests` / `aiohttp` → Async HTTP requests
* `BeautifulSoup4` → HTML parsing
* `ScrapingBee` → Cloudflare bypass
* `pandas` → CSV normalization
* `re` + `json` → Reverse engineering of embedded scripts

---

## 📂 Extracted Fields

* **Basic Info** → title, brand, SKU, UUID, slug
* **Pricing** → price, previousPrice, onSale, saleText
* **Categories** → Home → Smartphones → Samsung Galaxy → Product name
* **Images** → Up to 5 high-resolution URLs
* **Descriptions** → Long HTML description cleaned to single-line text
* **Specifications** → structured key/value attributes
* **Reviews** → content, rating, pros/cons, summary

---

## 🛠️ How It Works

1. **Cloudflare bypass** → all requests routed via ScrapingBee.
2. **HTML Parsing** → extract hidden JSON (`__NUXT_DATA__`, `ld+json`) when API unavailable.
3. **JS Reverse Engineering** → discovered API endpoints inside `*.js` bundles.
4. **Normalization** → convert nested JSON to clean CSV with consistent fields.
5. **Export** → final data stored in `products.csv`.

---

## 📊 Example Output

```json
{
  "sku": "08374b57-cca2-436c-8e7f-2363631beb2c",
  "name": "Coffee maker with grinder Sage The Barista Express BES875",
  "brand": "Sage",
  "price": 384.74,
  "previousPrice": 599.00,
  "onSale": "Y",
  "saleText": "save £214",
  "colour": "Silver",
  "cat": "Home",
  "subcat1": "Home appliances",
  "subcat2": "Small appliances",
  "desc": "A used item in reasonable condition. There will be cosmetic scratches...",
  "image1": "https://cdn.../image1.jpg",
  "image2": "https://cdn.../image2.jpg"
}
```

---

## 💡 Advantages

* Works **even when data is hidden behind JS or Cloudflare**.
* Flexible: can scrape **HTML, JSON, and API** sources.
* Reusable: endpoints reverse-engineered → usable for multiple products/categories.
* Clean structured output ready for analytics or database ingestion.

---

## 📌 Notes

* Requires valid **ScrapingBee API key**.
* Some API endpoints are **region-dependent** (may require `accept-language` header).
* For cart/checkout APIs, a logged-in session (cookies) is required.
