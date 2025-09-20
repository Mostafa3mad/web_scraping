# 🍻 Wetherspoon Android API Scraper

This project contains a collection of Python scripts to interact with the **Wetherspoon Android API**.  
It demonstrates how to extract **Venues, Sales Areas, Menus, and Products** programmatically.

---

## 📂 Project Structure

- **`1get_all_venus.py`** – Fetches all available venues using the `/venues` endpoint.  
- **`2data_of_venue_to_get_sales_area.py`** – Retrieves details of a specific venue (`venue_id`) and extracts its `salesArea_id`.  
- **`3all_mens_from_venue.py`** – Fetches all available menus for a given `venue_id` and `salesArea_id`.  
- **`4all_product_in_menu_categorie.py`** – Fetches all products inside a given menu, organized by categories and item groups.  

---

## ⚙️ Requirements

- Python 3.8+
- Install dependencies:
  ```bash
  pip install requests
````

---

## 🔑 Authentication

All requests require specific headers for authentication:

```http
Authorization: Bearer <YOUR_TOKEN>
User-Agent: Mozilla/5.0 (Linux; Android 14; ...)
```

Replace `<YOUR_TOKEN>` with a valid API token.

---

## 📌 Endpoints & Scripts

### 1️⃣ Get All Venues

**Script:** `1get_all_venus.py`
**Endpoint:**

```http
GET https://ca.jdw-apps.net/api/v0.1/venues
```

**Description:**
Fetches all venues. Extracts:

* `venueName`
* `venueRef` (used as `venue_id` in later requests)

---

### 2️⃣ Get Venue Details (Sales Areas)

**Script:** `2data_of_venue_to_get_sales_area.py`
**Endpoint:**

```http
GET https://ca.jdw-apps.net/api/v0.1/venues/{venue_id}
```

**Description:**
Fetches details of a specific venue and extracts:

* `salesArea_id`

---

### 3️⃣ Get All Menus for a Venue

**Script:** `3all_mens_from_venue.py`
**Endpoint:**

```http
GET https://ca.jdw-apps.net/api/v0.1/jdw/venues/{venue_id}/sales-areas/{salesArea_id}/menus?type=available
```

**Description:**
Fetches all available menus for the given venue and sales area. Extracts:

* `menuName`
* `menu_id`

---

### 4️⃣ Get Products from a Menu

**Script:** `4all_product_in_menu_categorie.py`
**Endpoint:**

```http
GET https://ca.jdw-apps.net/api/v0.1/jdw/venues/{venue_id}/sales-areas/{salesArea_id}/menus/{menu_id}
```

**Description:**
Fetches all products under the specified menu. Extracts:

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

Run each script individually:

```bash
python 1get_all_venus.py
python 2data_of_venue_to_get_sales_area.py
python 3all_mens_from_venue.py
python 4all_product_in_menu_categorie.py
```

---

## ⚠️ Notes

* You must replace `venue_id`, `salesArea_id`, and `menu_id` manually inside the scripts or automate chaining between them.
* This project is for **educational purposes only**.


