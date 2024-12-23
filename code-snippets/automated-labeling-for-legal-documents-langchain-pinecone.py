#I developed this code snippet to automate the labeling and tagging of legal documents using a Python script. It leverages LangChain and the Voyage Law-2 model to generate embeddings, which are then stored in a Pinecone vector database. This setup supports a Retrieval-Augmented Generation (RAG) system, with the added functionality to predefine and modify labels based on the specific type of data being processed.

import json
from tqdm import tqdm
import os
from pinecone import Pinecone
from config import (
    PINECONE_API_KEY, 
    PINECONE_ENVIRONMENT, 
    PINECONE_INDEX_NAME,
    ANTHROPIC_API_KEY,
    VOYAGE_API_KEY,
    DOCS_PATH
)
import time
from typing import Dict, List, Optional
import logging
from pathlib import Path
import asyncio
import aiofiles
from tagging.legal_text_tagger import EnhancedLegalTagger
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain_voyageai import VoyageAIEmbeddings
import re

# Define paths
INPUT_PATH = r"***/input"
OUTPUT_PATH = r"***/output"

class LegalDocumentProcessor:
    def __init__(self, docs_path: str, output_path: str):
        self.docs_path = docs_path
        self.output_path = output_path
        os.makedirs(self.output_path, exist_ok=True)

        self.tagger = EnhancedLegalTagger(anthropic_api_key=ANTHROPIC_API_KEY)
        self.embeddings = VoyageAIEmbeddings(
            voyage_api_key=VOYAGE_API_KEY,
            model="voyage-law-2",
            show_progress_bar=True
        )
        self.text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=500,
            chunk_overlap=200,
            length_function=len,
            is_separator_regex=False,
            separators=[
                "\n\n",  # Paragraph breaks
                "\n",    # Line breaks
                ". ",    # Sentences
            ],
            keep_separator=True
        )
        self.run_timestamp = int(time.time())
        self.run_id = f"clinical_negligence_{self.run_timestamp}"

    async def process_document(self, filename: str) -> List[Dict]:
        """Process document and prepare for Pinecone with unique IDs"""
        try:
            async with aiofiles.open(os.path.join(self.docs_path, filename), 'r', encoding='utf-8') as file:
                text = await file.read()

            text = re.sub(r'\s+', ' ', text)
            text = re.sub(r'([.!?])\s+', r'\1\n', text)
            chunks = self.text_splitter.split_text(text)

            seen_content = set()
            unique_chunks = []
            for chunk in chunks:
                clean_chunk = chunk.strip()
                if clean_chunk and clean_chunk not in seen_content:
                    seen_content.add(clean_chunk)
                    unique_chunks.append(chunk)

            return await self.process_chunks(unique_chunks, filename)

        except Exception as e:
            logging.error(f"Error processing document {filename}: {str(e)}")
            return []

    async def save_processed_data(self, processed_chunks: List[Dict]):
        """Save processed data to JSON with run identifier and detailed metadata"""
        try:
            output_file = os.path.join(self.output_path, f'processed_legal_data_{self.run_id}.json')
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(processed_chunks, f, indent=2)
            print(f"Saved processed data to {output_file}")

            metadata = {
                'run_id': self.run_id,
                'timestamp': self.run_timestamp,
                'processing_date': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(self.run_timestamp)),
                'statistics': {
                    'total_chunks': len(processed_chunks),
                    'total_documents': len(set(chunk['metadata']['source'] for chunk in processed_chunks)),
                    'categories': {},
                    'tagged_chunks': sum(1 for chunk in processed_chunks if chunk['metadata'].get('tags')),
                    'average_chunk_length': sum(len(chunk['text']) for chunk in processed_chunks) / len(processed_chunks) if processed_chunks else 0
                }
            }

            for chunk in processed_chunks:
                category = chunk['metadata'].get('category')
                if category:
                    metadata['statistics']['categories'][category] = metadata['statistics']['categories'].get(category, 0) + 1

            metadata_file = os.path.join(self.output_path, f'run_metadata_{self.run_id}.json')
            with open(metadata_file, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, indent=2)
            print(f"Saved run metadata to {metadata_file}")

        except Exception as e:
            logging.error(f"Error saving processed data: {str(e)}")
            raise

    async def process_all_documents(self):
        """Process all documents with run tracking"""
        input_dir = Path(self.docs_path)
        files = list(input_dir.glob('*.txt'))
        all_processed_chunks = []

        print(f"\nStarting processing run: {self.run_id}")
        print(f"Timestamp: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(self.run_timestamp))}")

        with tqdm(total=len(files), desc="Processing documents") as pbar:
            for file in files:
                chunks = await self.process_document(file.name)
                all_processed_chunks.extend(chunks)
                pbar.update(1)

        await self.save_processed_data(all_processed_chunks)
        return len(all_processed_chunks)

    async def process_chunks(self, chunks: List[str], filename: str) -> List[Dict]:
        """Process chunks with tagging and embeddings"""
        processed_chunks = []
        document_id = f"{self.run_id}_{filename.replace('.txt', '')}"

        for i, chunk in enumerate(chunks):
            try:
                chunk_id = f"{document_id}_chunk_{i}"
                tags = self.get_custom_tags(chunk)
                embedding = await self.get_embedding(chunk)

                processed_chunk = {
                    "id": chunk_id,
                    "text": chunk,
                    "embedding": embedding,
                    "metadata": {
                        "source": filename,
                        "chunk_index": i,
                        "tags": tags,
                        "run_id": self.run_id,
                        "processing_timestamp": self.run_timestamp,
                        "document_id": document_id
                    }
                }

                processed_chunks.append(processed_chunk)

            except Exception as e:
                logging.error(f"Error processing chunk {i} of {filename}: {str(e)}")
                continue

        return processed_chunks

    def get_custom_tags(self, text: str) -> List[str]:
        """Automatically generate tags based on text content"""
        tags = []
        if "negligence" in text.lower():
            tags.append("negligence")
        if "contract" in text.lower():
            tags.append("contract")
        if "medical" in text.lower():
            tags.append("medical")
        return tags

    async def get_embedding(self, text: str) -> List[float]:
        """Generate embedding for text"""
        try:
            embedding = await self.embeddings.aembed_query(text)
            return embedding
        except Exception as e:
            logging.error(f"Error generating embedding: {str(e)}")
            raise

async def main():
    try:
        processor = LegalDocumentProcessor(docs_path=INPUT_PATH, output_path=OUTPUT_PATH)
        total_chunks = await processor.process_all_documents()
        print(f"\nProcessed {total_chunks} chunks")

        print("\nStarting Pinecone upload...")
        from upload_to_pinecone import upload_processed_data_to_pinecone
        upload_processed_data_to_pinecone(processor.run_id, OUTPUT_PATH)

    except Exception as e:
        logging.error(f"Error in main processing: {str(e)}", exc_info=True)
        raise

if __name__ == "__main__":
    asyncio.run(main())