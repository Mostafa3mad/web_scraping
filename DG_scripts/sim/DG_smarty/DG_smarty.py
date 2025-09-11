import time

from functions import *
import os
from html import unescape
from bs4 import BeautifulSoup
import urllib.parse
import html






if DEFAULT_CONFIG["save_local"]:

    for folder in [DATA_DIR, SITEMAPS_DIR, PRODUCTS_DIR,OUTPUTS_DIR ]:
        os.makedirs(folder, exist_ok=True)

logger = setup_logger("logs/scraper.log")




def get_standard_csv_headers():
    headers = [
        "source", "date", "apiURL", "url", "sku", "name", "brand","stock",
        "advance","paymentAmount","phoneContractDuration","sim_price","simContractname","simContractDuration","phoneContractPrice","isPhoneContractAvailableWOsim"
        ,"phoneContractSimPackage","handsetOnlyCostCash","handsetOnlyContract",
        "previousPrice", "onSale", "saleText",
        "plan_type","sim_data","simOfferData", "sim1YearIncrease", "sim2YearIncrease", "sim3YearIncrease","simDesc",
        "colour", "size", "UPC", "EAN",
        "cat", "subcat1", "subcat2", "subcat3", "subcat4", "subcat5", "warranty",
        "image1", "image2", "image3", "image4", "image5", "desc", "shortDesc",
        "reviewCount", "reviewRating", "videoURL", "isSellingFast",
        "isRestockingSoon", "isPromotion", "isOutletPrice", "lowestPriceText",
        "lowestPriceValue"
    ]
    for i in range(1, 21):
        headers += [f"attributeType{i}", f"attributeTitle{i}", f"attributeValue{i}"]
    return headers

def create_csv_file(filepath):
    if DEFAULT_CONFIG["save_local"]:

        headers = get_standard_csv_headers()
        with open(OUTPUTS_DIR/filepath, mode="w", newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()

def append_to_csv(item, filepath):

    if DEFAULT_CONFIG["save_local"]:

        headers = get_standard_csv_headers()
        with open(OUTPUTS_DIR/filepath, mode="a", newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writerow(item)


def format_offer(voucher: Optional[dict]) -> str:
    if not voucher:
        return ""
    data_amount = voucher.get("applies_to").get("title")

    return data_amount
def offer_chip_text(voucher: Optional[dict]) -> str:
    if not voucher:
        return ""
    offer_chip_text = voucher.get("roundel_type")

    return offer_chip_text

def _best_data_gb(plan: dict):
    return plan.get("dataAllowanceWithPromosGB") if plan.get("dataAllowanceWithPromosGB") is not None else plan.get("dataAllowanceGB")

def _int_if_whole(x):
    if isinstance(x, float) and x.is_integer():
        return int(x)
    return x

async def fetch_single_product(url: str) -> List[Dict[str, Any]]:
    # 1) اجلب الداتا
    response = await fetch_url(url, content_type="product")

    if isinstance(response, (bytes, str)):
        try:
            payload = json.loads(response)
        except json.JSONDecodeError as e:
            print("JSONDecodeError:", e)
            print("First 300 chars of response:", str(response)[:300])
            return []
    elif isinstance(response, dict):
        payload = response
    else:
        print("Unexpected response type:", type(response))
        return []

    try:
        plans = payload["data"]["attributes"]["plans"]
    except KeyError as e:
        print("Missing key in payload:", e)
        print("Top-level keys:", list(payload.keys()) if isinstance(payload, dict) else type(payload))
        return []

    plan_sims: List[Dict[str, Any]] = []

    for idx, plan in enumerate(plans, start=1):
        if not isinstance(plan, dict):
            print(f"Plan #{idx} is not a dict, got {type(plan)}")
            continue

        plan_type = plan.get("categoryType") or ""
        sim_data = _best_data_gb(plan)
        sim_price = (plan.get("finalPrice") or {}).get("value")
        url_plan = f"https://smarty.co.uk/plans/{plan.get('slug')}" if plan.get("slug") else ""
        plan_sim: Dict[str, Any] = {
            "source": "smarty",
            "date": time.strftime("%Y-%m-%d"),
            "apiURL": url,

            "url": url_plan,
            "sku": plan.get("id") or "",
            "plan_type": plan_type,
            "sim_data": _int_if_whole(sim_data) if sim_data is not None else sim_data,
            "sim_price": _int_if_whole(sim_price),
            "simOfferData": format_offer(plan.get("voucher")),
            "saleText" : offer_chip_text(plan.get("voucher")),
            "onSale" : "Y" if offer_chip_text(plan.get("voucher")) else "",
            "simContractname": plan.get("name") or "",
            "simContractDuration": 1,
            "isPhoneContractAvailableWOsim": "N",
            "phoneContractSimPackage": "",
            "handsetOnlyContract": "",
            "sim1YearIncrease": "",
            "sim2YearIncrease": "",
            "sim3YearIncrease": "",
            "simDesc": plan.get("description") or "",
            "cat" : url_plan.split("/")[3],
            "subcat1" : url_plan.split("/")[4] if len(url_plan.split("/")) > 4 else "",
            "subcat2" : url_plan.split("/")[5] if len(url_plan.split("/")) > 5 else "",
            "subcat3" : url_plan.split("/")[6] if len(url_plan.split("/")) > 6 else "",
            "subcat4" : url_plan.split("/")[7] if len(url_plan.split("/")) > 7 else "",
            "subcat5" : url_plan.split("/")[8] if len(url_plan.split("/")) > 8 else "",
        }


        # print(plan_sim)
        append_to_csv(plan_sim, "products.csv")


    return plan_sims









async def extact_data_from_product_url(all_product_urls: list[str]):
    tasks = []
    total_urls = len(all_product_urls)

    with tqdm(total=total_urls, desc="Processing product URLs", ncols=100) as pbar:
        for url in all_product_urls:
            task = asyncio.create_task(wrapped_fetch(url))
            tasks.append(task)
            pbar.update(1)
            if len(tasks) >= DEFAULT_WORKERS:
                await tasks.pop(0)

    for task in tasks:
        await task



async def wrapped_fetch(url):
    try:
        await fetch_single_product(url)
    except Exception as e:
        logger = logging.getLogger("scraper")
        logger.warning(f"Failed to fetch {url}: {e}")








async def main():
    create_csv_file("products.csv")
    url = ["https://smarty.co.uk/api/v3/plans?voucher_code="]
    data = await extact_data_from_product_url(url)


if __name__ == "__main__":
    asyncio.run(main())
