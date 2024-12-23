import os
import time
import asyncio
import logging
from typing import List, Dict

import aiohttp
from dotenv import load_dotenv
from langchain.llms import LlamaCpp
from langchain.embeddings import LlamaCppEmbeddings
from langchain.text_splitter import CharacterTextSplitter
from langchain.vectorstores import Pinecone
from langchain.document_loaders import WebBaseLoader
from scraperapi_sdk import ScraperAPIClient
from bs4 import BeautifulSoup
from ratelimit import limits, sleep_and_retry
import pinecone

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Loading environment variables from .env file
load_dotenv()

# Validate environment variables
REQUIRED_ENV_VARS = ["SCRAPERAPI_KEY", "PINECONE_API_KEY", "PINECONE_ENVIRONMENT"]
missing_vars = [var for var in REQUIRED_ENV_VARS if not os.getenv(var)]
if missing_vars:
    raise EnvironmentError(f"Missing required environment variables: {', '.join(missing_vars)}")

# Initialize ScraperAPI client
scraper = ScraperAPIClient(os.getenv("SCRAPERAPI_KEY"))

# Initialize Llama model
llama_model_path = os.getenv("LLAMA_MODEL_PATH", "***/model.bin")
if not os.path.exists(llama_model_path):
    raise FileNotFoundError(f"Llama model file not found at {llama_model_path}")

llm = LlamaCpp(model_path=llama_model_path, n_ctx=2048, n_batch=8)
embeddings = LlamaCppEmbeddings(model_path=llama_model_path)

# Initialize Pinecone
pinecone.init(api_key=os.getenv("PINECONE_API_KEY"), environment=os.getenv("PINECONE_ENVIRONMENT"))
index_name = "facebook-business-pages"
if index_name not in pinecone.list_indexes():
    pinecone.create_index(index_name, dimension=4096)
pinecone_index = pinecone.Index(index_name)

# Rate limiting decorator
@sleep_and_retry
@limits(calls=5, period=60)
def rate_limited_scraper_get(url: str) -> str:
    return scraper.get(url).text

async def scrape_facebook_page(url: str) -> Dict[str, str]:
    """
    Scrape a Facebook business page and extract relevant information.

    Args:
    url (str): The URL of the Facebook business page to scrape.

    Returns:
    Dict[str, str]: A dictionary containing the extracted information.
    """
    try:
        html_content = await asyncio.to_thread(rate_limited_scraper_get, url)
        soup = BeautifulSoup(html_content, 'html.parser')

        # Extract data fields
        name = soup.find('h1', {'class': 'x1heor9g x1qlqyl8 x1pd3egz x1a2a7pz'})
        name = name.text.strip() if name else "N/A"

        about = soup.find('div', {'class': 'x1yztbdb'})
        about = about.text.strip() if about else "N/A"

        return {
            "name": name,
            "about": about,
        }
    except Exception as e:
        logging.error(f"Error scraping {url}: {str(e)}")
        return {}

async def process_and_store(data: Dict[str, str]):
    """
    Process the scraped data and store it in Pinecone.

    Args:
    data (Dict[str, str]): The scraped data to process and store.
    """
    try:
        text = "\n".join([f"{k}: {v}" for k, v in data.items()])

        text_splitter = CharacterTextSplitter(chunk_size=1000, chunk_overlap=0)
        texts = text_splitter.split_text(text)

        await asyncio.to_thread(Pinecone.from_texts, [t for t in texts], embeddings, index_name=index_name)

        logging.info(f"Stored data for: {data.get('name', 'Unknown')}")
    except Exception as e:
        logging.error(f"Error processing and storing data: {str(e)}")

async def main():
    facebook_urls = [
        
        # Facebook business page URLs
    ]

    tasks = [scrape_facebook_page(url) for url in facebook_urls]
    results = await asyncio.gather(*tasks)

    store_tasks = [process_and_store(data) for data in results if data]
    await asyncio.gather(*store_tasks)

if __name__ == "__main__":
    asyncio.run(main())
