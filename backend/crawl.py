import requests
import json
import time
from typing import List

def crawl_urls(urls: List[str], timeout: int = 20) -> str:
    """
    Gửi request crawl đến API
    """
    api_url = "http://localhost:8000/api/crawl"
    payload = {
        "urls": urls,
        "timeout": timeout
    }
    
    response = requests.post(api_url, json=payload)
    if response.status_code == 200:
        return response.json()["task_id"]
    else:
        raise Exception(f"Error: {response.text}")

def check_task_status(task_id: str) -> dict:
    """
    Kiểm tra trạng thái của task
    """
    api_url = f"http://localhost:8000/api/task/{task_id}"
    response = requests.get(api_url)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Error: {response.text}")

def get_crawled_data(limit: int = 10) -> dict:
    """
    Lấy dữ liệu đã crawl
    """
    api_url = f"http://localhost:8000/api/crawled-data?limit={limit}"
    response = requests.get(api_url)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Error: {response.text}")

def main():
    # Danh sách URL cần crawl
    urls = [
        "https://nhandan.vn/",
        "https://vietnamplus.vn/",
        "https://tuoitre.vn/",
        "https://thanhnien.vn/"
    ]
    
    try:
        # Gửi request crawl
        print("Đang gửi request crawl...")
        task_id = crawl_urls(urls)
        print(f"Task ID: {task_id}")
        
        # Kiểm tra trạng thái task
        while True:
            status = check_task_status(task_id)
            print(f"Task status: {status}")
            
            if status["state"] in ["SUCCESS", "FAILURE"]:
                break
                
            time.sleep(5)  # Đợi 5 giây trước khi kiểm tra lại
        
        # Lấy dữ liệu đã crawl
        print("\nDữ liệu đã crawl:")
        data = get_crawled_data()
        print(json.dumps(data, indent=2, ensure_ascii=False))
        
    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    main() 