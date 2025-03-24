# DVA318 SACRAL

# Instructions
1. Run crawler
2. Run populate database
3. Run main
Also:
You need to fetch LLM and embedding models from the ollama website. For our project we used:
Embeddings:
mxbai-embed-large
Definity/snowflake-arctic-embed-l-v2.0-q8_0

LLM:
mistral:7b
llama3.1:8b
deepseek-r1:7b

# How to run crawler:
``
python mdu_unified_crawler.py --course-range 25000 33000 --program-range 500 2000 --no-delay  --verbose
``

# How to run populate database

Choose an embedding model and replace the "embed_model" to the embed model chosen.
Run the python script.

# How to run main
Choose LLM, embedding, path to chromadb
