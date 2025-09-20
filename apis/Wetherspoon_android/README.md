# 🍻 Wetherspoon Android API Scraper

This project is a **showcase of my advanced scraping and reverse engineering skills**.  
It demonstrates how I analyzed and reversed the **Wetherspoon Android API**, bypassed SSL protections, and built Python scripts to programmatically extract structured data including **Venues, Sales Areas, Menus, and Products**.

---

## 👨‍💻 About Me

I am a **Scraper & Automation Specialist** with strong experience in both **Web** and **Android** environments:

- 📱 **Android API Scraping** – Reverse-engineering mobile apps to extract hidden/private APIs.  
- 🔓 **Bypassing Protections** – SSL pinning bypass, certificate verification, and app-layer security.  
- 🌍 **Web Scraping** – Static & dynamic websites (JavaScript-heavy) with **Requests, Selenium, Playwright**.  
- ⚡ **Automation & Bots** – Automating repetitive tasks with clean and reusable scripts.  
- 📊 **Data Extraction & Export** – Delivering structured data (JSON, CSV, Excel, or databases).  

---

## 📂 Project Overview

This repo contains 4 Python scripts, each representing a step in the scraping pipeline:

- **`1get_all_venus.py`** – Fetches all available venues using the `/venues` endpoint.  
- **`2data_of_venue_to_get_sales_area.py`** – Retrieves details of a specific venue (`venue_id`) and extracts its `salesArea_id`.  
- **`3all_mens_from_venue.py`** – Fetches all available menus for a given `venue_id` and `salesArea_id`.  
- **`4all_product_in_menu_categorie.py`** – Scrapes all products inside a given menu, organized by categories and item groups.  

Each script is modular, so you can run them independently or chain them together for a full scraping workflow.

---

## ⚙️ Tech Stack

- **Python 3.8+**  
- **Requests** (HTTP client)  
- Reverse-engineered API endpoints from Android app traffic  
- Custom JSON parsing & structured data export  

Install dependencies:

```bash
pip install requests
````

---

## 🔑 Authentication

All API calls use headers for authentication:

```http
Authorization: Bearer <YOUR_TOKEN>
User-Agent: Mozilla/5.0 (Linux; Android 14; ...)
```

Replace `<YOUR_TOKEN>` with a valid API token.

---

## 📌 Endpoints & Functionality

### 1️⃣ Get All Venues

**Script:** `1get_all_venus.py`
**Endpoint:**

```http
GET https://ca.jdw-apps.net/api/v0.1/venues
```

➡️ Extracts: `venueName`, `venueRef`

---

### 2️⃣ Get Venue Details (Sales Areas)

**Script:** `2data_of_venue_to_get_sales_area.py`
**Endpoint:**

```http
GET https://ca.jdw-apps.net/api/v0.1/venues/{venue_id}
```

➡️ Extracts: `salesArea_id`

---

### 3️⃣ Get All Menus for a Venue

**Script:** `3all_mens_from_venue.py`
**Endpoint:**

```http
GET https://ca.jdw-apps.net/api/v0.1/jdw/venues/{venue_id}/sales-areas/{salesArea_id}/menus?type=available
```

➡️ Extracts: `menuName`, `menu_id`

---

### 4️⃣ Get Products from a Menu

**Script:** `4all_product_in_menu_categorie.py`
**Endpoint:**

```http
GET https://ca.jdw-apps.net/api/v0.1/jdw/venues/{venue_id}/sales-areas/{salesArea_id}/menus/{menu_id}
```

➡️ Extracts:

* `categoryName`
* `itemGroup_name`
* `productName`
* `port_size`
* `priceValue`
* `productId`

**Example Output:**

```text
------------------
Category Name:  Chicken
Item Group Name:  Chicken baskets
Product Name:  Boneless basket
Product port_size:  Standard
Price Value:  12.72
Product ID:  10000141585
```

---

## 🚀 Usage

Run scripts individually:

```bash
python 1get_all_venus.py
python 2data_of_venue_to_get_sales_area.py
python 3all_mens_from_venue.py
python 4all_product_in_menu_categorie.py
```

Or chain them together to scrape the full dataset: **Venues → Sales Areas → Menus → Products**.

---

## ⚡ Why Work With Me?

✅ Reverse-engineering Android apps
✅ Bypassing SSL pinning & protection layers
✅ Extracting hidden/private APIs
✅ Web scraping (static + dynamic)
✅ Automation bots & data pipelines
✅ Clean, maintainable Python code

---

## 🌟 Portfolio Use Cases

I have worked on scraping and automation projects in multiple industries:

* 🛒 **E-commerce** – Product catalogs, prices, reviews.
* ✈️ **Travel** – Flight data, hotel availability, booking APIs.
* 🎮 **Entertainment** – Game/app APIs, leaderboards, hidden endpoints.
* 📊 **Business Intelligence** – Market research, competitor monitoring.
* 🍔 **Food & Delivery Apps** – Menus, pricing, and product catalogs.

---

## 🤝 Hire Me

Looking for someone who can **scrape Android APIs, bypass SSL protections, or extract data from complex websites**?
I’ll deliver **secure, reliable, and production-ready scraping solutions**.

📬 [👉 View my Upwork Profile](https://www.upwork.com/freelancers/~0179f2b4933834b31f)

Let’s work together and turn your data challenges into clean, structured solutions 🚀
