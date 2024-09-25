import fitz  # PyMuPDF
import pytesseract
from PIL import Image
import pinecone
import openai

def extract_text_from_pdf(pdf_path):
    text = ""
    doc = fitz.open(pdf_path)
    for page in doc:
        text += page.get_text()
    return text

def extract_text_from_image(image_path):
    img = Image.open(image_path)
    text = pytesseract.image_to_string(img)
    return text

def extract_text_from_file(file_path):
    with open(file_path, 'r') as file:
        return file.read()

pinecone.init(api_key='*****', environment='****')

index = pinecone.Index('my-index')

def store_in_knowledge_base(text, metadata):
    # Converting text into embedding using OpenAI
    embedding = get_text_embedding(text)
    index.upsert(vectors=[(text, embedding, metadata)])

def retrieve_documents(query):
    # Convert query into embedding
    query_embedding = get_text_embedding(query)
    
    # Retrieve top-k documents
    result = index.query(query_embedding, top_k=5, include_metadata=True)
    return result['matches']

openai.api_key = '****'

def generate_response(query, retrieved_docs):
    # Format the prompt by combining query and retrieved docs
    context = "\n".join([doc['metadata']['text'] for doc in retrieved_docs])
    prompt = f"Answer the following query based on this context:\nContext: {context}\nQuery: {query}"

    response = openai.Completion.create(
        engine="gpt-4",
        prompt=prompt,
        max_tokens=200,
        temperature=0.7
    )
    return response.choices[0].text.strip()

# Creating Multiple Agents

class LLM_Agent:
    def __init__(self, knowledge_base_name):
        self.knowledge_base = pinecone.Index(knowledge_base_name)
    
    def handle_query(self, query):
        retrieved_docs = retrieve_documents(query)
        return generate_response(query, retrieved_docs)

# Create agents
agent_1 = LLM_Agent('health-knowledge')
agent_2 = LLM_Agent('finance-knowledge')

# Asking each agent a separate question
response_health = agent_1.handle_query("What are the symptoms of flu?")
response_finance = agent_2.handle_query("What are the best investment options?")