import streamlit as st
from dotenv import load_dotenv
from utils import query_agent

load_dotenv()


st.title("Search Patient Records")
st.header("Search Patient Records")

query = st.text_area("Enter your query")
button = st.button("Show Details")

if button:
    # Get Response
    answer =  query_agent(query)
    st.write(answer)