import pandas as pd
import spacy
import re
from collections import Counter
import openai
import os
import matplotlib.pyplot as plt
from wordcloud import WordCloud

openai.api_key = os.getenv("MEDPALM2_API_KEY")


# Preparing the Medical Text Data
df = pd.read_csv('*****.csv')

# Viewing the dataset
print(df.head())

#Preprocessing the Medical Text Data

# Loading the spaCy English model
nlp = spacy.load("en_core_web_sm")

def preprocess_text(text):
    # Clean special characters, digits, and unnecessary spaces
    text = re.sub(r'\W+', ' ', text)
    text = re.sub(r'\d+', '', text)
    doc = nlp(text)
    
    # Tokenize, remove stopwords, and lemmatize the text
    tokens = [token.lemma_ for token in doc if not token.is_stop and not token.is_punct]
    return ' '.join(tokens)

# Preprocessing the 'text_summary' column in the dataset
df['cleaned_text_summary_summary'] = df['text_summary'].apply(preprocess_text)

# Viewing the cleaned data
print(df[['text_summary', 'cleaned_text_summary_summary']].head())

#extracting relevant entities using spaCy’s pre-trained NER model

def extract_entities(text):
    doc = nlp(text)
    entities = [(ent.text, ent.label_) for ent in doc.ents]
    return entities

# Apply NER on the cleaned text
df['entities'] = df['cleaned_text_summary_summary'].apply(extract_entities)

# View extracted entities
print(df[['cleaned_text_summary_summary', 'entities']].head())

# Extract keywords based on word frequency
def extract_keywords(text):
    doc = nlp(text)
    keywords = [token.text for token in doc if token.is_alpha]
    keyword_freq = Counter(keywords)
    return keyword_freq.most_common(5)  # Return the top 5 keywords

# Apply keyword extraction
df['keywords'] = df['cleaned_text_summary_summary'].apply(extract_keywords)

# View keywords
print(df[['cleaned_text_summary_summary', 'keywords']].head())

#Querying the MED Palm 2 API

def analyze_text_with_medpalm2(text):
    response = openai.Completion.create(
        engine="med-palm-2",  
        prompt=text,
        max_tokens=150,
        temperature=0.7,
        n=1
    )
    return response['choices'][0]['text'].strip()

# Apply MED Palm 2 API to analyze the cleaned text
df['medpalm2_analysis'] = df['cleaned_text_summary_summary'].apply(analyze_text_with_medpalm2)

# View the analysis results
print(df[['cleaned_text_summary_summary', 'medpalm2_analysis']].head())

#Medical Text Summarization

def summarize_medical_text(text):
    prompt = f"Summarize the following medical document: {text}"
    response = analyze_text_with_medpalm2(prompt)
    return response

# Apply summarization on the medical text
df['summary'] = df['cleaned_text_summary'].apply(summarize_medical_text)

# View summaries
print(df[['cleaned_text_summary', 'summary']].head())

#Classifying text into categories like diagnosis, symptoms, treatments

def classify_medical_text(text):
    prompt = f"Classify the following medical text: {text}"
    response = analyze_text_with_medpalm2(prompt)
    return response

# Apply classification
df['classification'] = df['cleaned_text_summary'].apply(classify_medical_text)

# View classifications
print(df[['cleaned_text_summary', 'classification']].head())

#Visualizing Key Terms

# Combine all keywords for a word cloud
all_keywords = ' '.join([' '.join([k for k, v in row]) for row in df['keywords']])

# Generate the word cloud
wordcloud = WordCloud(width=800, height=400, background_color='white').generate(all_keywords)

# Display the word cloud
plt.figure(figsize=(10, 5))
plt.imshow(wordcloud, interpolation='bilinear')
plt.axis("off")
plt.show()
