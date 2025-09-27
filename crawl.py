import asyncio
import aiohttp
import aiofiles
import csv
from asyncio import Semaphore
from pathlib import Path
import logging
import random

# API endpoint
BASE_URL = "https://s6.tuoitre.vn/api/diem-thi-thpt.htm"

# Cấu hình
CONCURRENCY = 10  # Tránh 429
BATCH_SIZE = 200  # Số lượng SBD mỗi batch
TIMEOUT = 10
DELAY_BETWEEN_REQUESTS = 0.5
MAX_CONSECUTIVE_MISSES = 1000
SERIAL_DIGITS = 6
OUTPUT_DIR = "data_2025"
RATE_LIMIT_THRESHOLD = 0.5

# Mã tỉnh từ 01 đến 64
PROVINCE_CODES = [f"{i:02d}" for i in range(1, 65)]

# Cấu hình logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def build_sbd(province: str, serial: int) -> str:
    return f"{province}{serial:0{SERIAL_DIGITS}d}"

def build_request_url(sbd: str, year: int) -> str:
    return f"{BASE_URL}?sbd={sbd}&year={year}"

async def fetch(session: aiohttp.ClientSession, url: str, retries: int = 3) -> dict:
    for attempt in range(retries):
        try:
            async with session.get(url, timeout=TIMEOUT) as resp:
                if resp.status == 200:
                    json_data = await resp.json()
                    if json_data.get("success") and json_data.get("total", 0) > 0:
                        logger.info(f"Thành công cho {url}: total={json_data['total']}")
                        return json_data
                    else:                                                                
                        logger.debug(f"Không có dữ liệu cho {url}")
                        return {"success": False}
                elif resp.status == 429:
                    wait_time = (attempt + 1) * 10 + random.uniform(0, 2)
                    logger.warning(f"HTTP 429 cho {url}, chờ {wait_time:.2f}s")
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    logger.warning(f"HTTP {resp.status} cho {url}")
                    await asyncio.sleep(0.5 + random.uniform(0, 0.2))
                    continue
        except asyncio.TimeoutError:
            logger.warning(f"Timeout (lần {attempt+1}) cho {url}")
            await asyncio.sleep(1 + random.uniform(0, 0.2))
            continue
        except aiohttp.ClientError as e:
            logger.warning(f"ClientError cho {url}: {e}")
            await asyncio.sleep(0.5 + random.uniform(0, 0.2))
            continue
    return {"success": False}

async def fetch_batch(session: aiohttp.ClientSession, province: str, start_serial: int, batch_size: int, year: int, sem: Semaphore) -> list:
    tasks = []
    sbds = [build_sbd(province, serial) for serial in range(start_serial, start_serial + batch_size)]
    
    async with sem:
        for sbd in sbds:
            url = build_request_url(sbd, year)
            tasks.append(fetch(session, url))
        results = await asyncio.gather(*tasks, return_exceptions=True)
    
    subjects = ["TOAN", "VAN", "NGOAI_NGU", "LI", "HOA", "SINH", "SU", "DIA", "GIAO_DUC_CONG_DAN", "TIN_HOC", "CN_CONG_NGHIEP", "CN_NONG_NGHIEP", "GDKT_PL", "TONGDIEM"]
    valid_results = []
    rate_limit_count = 0
    
    for sbd, result in zip(sbds, results):
        if isinstance(result, Exception) or not result.get("success"):
            rate_limit_count += 1
            continue
        for item in result.get("data", []):
            valid_results.append({
                "sbd": sbd,
                "tinh_id": item.get("TinhId", province),
                "data": {subj: item.get(subj, -1) for subj in subjects}
            })
    
    if rate_limit_count / batch_size >= RATE_LIMIT_THRESHOLD:
        logger.warning(f"[{province}] Batch {start_serial} có {rate_limit_count}/{batch_size} lỗi 429, tạm dừng 60s")
        await asyncio.sleep(60)
    
    return valid_results

async def worker_scan_province(province: str, sem: Semaphore, session: aiohttp.ClientSession, year: int):
    output_file = Path(OUTPUT_DIR) / f"{province}.csv"
    subjects = ["TOAN", "VAN", "NGOAI_NGU", "LI", "HOA", "SINH", "SU", "DIA", "GIAO_DUC_CONG_DAN", "TIN_HOC", "CN_CONG_NGHIEP", "CN_NONG_NGHIEP", "GDKT_PL", "TONGDIEM"]
    total = 0
    consecutive_misses = 0
    serial = 1
    max_serial = 150000

    if not output_file.exists():
        async with aiofiles.open(output_file, mode="w", encoding="utf-8", newline='') as f:
            writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
            await f.write('\ufeff')
            await writer.writerow(["SBD", "TinhId"] + subjects)

    while serial <= max_serial:
        batch_results = await fetch_batch(session, province, serial, BATCH_SIZE, year, sem)
        await asyncio.sleep(DELAY_BETWEEN_REQUESTS)

        if batch_results:
            consecutive_misses = 0
            total += len(batch_results)
            async with aiofiles.open(output_file, mode="a", encoding="utf-8", newline='') as f:
                writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
                for item in batch_results:
                    await writer.writerow([item["sbd"], item["tinh_id"]] + [item["data"][subj] for subj in subjects])
            print(f"[{province}] Tìm thấy {len(batch_results)} bản ghi tại batch {serial}")
        else:
            consecutive_misses += BATCH_SIZE

        if consecutive_misses >= MAX_CONSECUTIVE_MISSES:
            print(f"[{province}] Dừng sau {consecutive_misses} miss (serial cuối: {serial})")
            break

        serial += BATCH_SIZE

    print(f"[{province}] Hoàn thành: {total} bản ghi vào {output_file}")

async def main():
    Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)
    sem = Semaphore(CONCURRENCY)
    timeout = aiohttp.ClientTimeout(total=None, sock_connect=TIMEOUT, sock_read=TIMEOUT)

    async with aiohttp.ClientSession(timeout=timeout) as session:
        for province in PROVINCE_CODES:
            await worker_scan_province(province, sem, session, year=2025)

if __name__ == "__main__":
    asyncio.run(main())