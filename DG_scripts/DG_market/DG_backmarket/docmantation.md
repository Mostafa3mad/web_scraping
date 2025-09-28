# 📖 Backmarket Scraper – Documentation

## 1. Overview

الاسكربت  معمول علشان يسحب بيانات المنتجات من موقع **Backmarket** (refurbished electronics).
بيستخدم **asyncio + httpx** عشان parallel requests، وبيخزن البيانات في CSV بستايل موحد.

---

## 2. Project Structure

```
├── DG_backmarket.py   # main scraper logic
├── functions.py       # utility functions, classes, helpers
├── products.csv       # output file (product data)
└── data/              # caching & raw data (sitemaps/products/categories)
```

---

## 3. Main Components

### a) **functions.py**

* **Constants & Config**

  * `DEFAULT_CONFIG`: إعدادات زي `save_local`, `use_scrapingbee`, `workers`.
  * `SCRAPINGBEE_URL` + key للتعامل مع ScrapingBee.

* **Utilities**

  * `setup_logger`: Logging system.
  * `get_random_headers`: User-Agent randomizer لتفادي الحظر.
  * `get_cache_path`: Path لكل صفحة cached (sitemap / category / product).
  * `fetch_url`: Async fetch مع retries + caching.
  * `fetch_with_scrapingbee`: لو شغلت proxy.
  * `fetch_sitemap`: يجيب كل الـ product URLs من XML sitemap.
  * `save_to_file`, `append_to_file`: Helpers لتخزين JSON/Text.

* **Data Models**

  * `Product`: Class يمثل منتج بخصائصه + تحويله إلى dict / CSV / pipe-delimited.
  * `Category`, `Brand`: Structs بسيطة للتصنيفات والماركات.

* **CSV Handling**

  * `save_to_csv`: يكتب records للملف مع deduplication.
  * `append_to_csv`, `append_to_csv_with_deduplication`.
  * `append_to_delimited_file_with_deduplication`.

* **Scraper Workflow**

  * `generic_product_worker`: Async worker يسحب و يعالج منتجات من Queue.
  * `run_scraper`: Pipeline كامل (fetch sitemap → extract categories/brands → scrape products → save results).

---

### b) **DG_backmarket.py**

* **CSV Schema**

  * `get_standard_csv_headers`: بيرجع الأعمدة الموحدة (source, sku, name, price, desc, … + attributes).
  * `create_csv_file`, `append_to_csv`: تجهيز / إضافة البيانات للـ CSV.

* **Scraper Functions**

  * `fetch_pickers`: يجيب الـ product variants.
  * `fetch_data_produt`: API call يجيب تفاصيل المنتج.
  * `fetch_review`: يجيب تقييمات المنتج.
  * `fetch_specifications`: يجيب الـ technical specs.
  * `get_categories`: يقرأ breadcrumb من HTML.
  * `fetch_single_product`: أهم دالة:

    * يجيب HTML.
    * يستخرج UUID.
    * يجيب variants.
    * لكل variant: يجيب السعر، الصور، الـ specs، reviews، desc.
    * يحفظ record في CSV.
  * `extract_links`: يجيب كل الـ product URLs من sitemaps.
  * `extact_data_from_product_url`: يـ process كل URL بالـ tasks.
  * `wrapped_fetch`: Wrapper مع error handling.

* **Main Entry**

  * `main`: يبدأ بالـ sitemap URLs (`sitemap_products.xml`, `sitemap_master_product_pages.xml`) → يجيب المنتجات → يحفظها في `products.csv`.

---

## 4. Output Schema

ملف **products.csv** يحتوي أعمدة زي:

* **Basic Info**: `source`, `date`, `url`, `sku`, `name`, `brand`.
* **Pricing**: `price`, `previousPrice`, `onSale`, `saleText`, `lowestPriceText`.
* **Categories**: `cat`, `subcat1..subcat5`.
* **Media**: `image1`–`image5`.
* **Descriptions**: `desc`, `shortDesc`.
* **Reviews**: `reviewCount`, `reviewRating`.
* **Attributes**: `attributeType1..20`, `attributeTitle1..20`, `attributeValue1..20`.

---

## 5. Workflow

1. Fetch **sitemaps** → get product URLs.
2. For each product:

   * Parse HTML + meta data.
   * Extract UUID & call APIs (`pickers`, `specifications`, `reviews`).
   * Collect images (up to 5).
   * Build record with attributes/specs.
   * Append to CSV.
3. Handle retries + caching locally.

---

## 6. Extensibility

* ممكن تغير عدد العمالين (workers) في `DEFAULT_CONFIG`.
* لو عايز تضيف لغة/ماركت غير UK → تغير `spb-accept-language` headers.
* CSV schema جاهز يستوعب attributes إضافية.

