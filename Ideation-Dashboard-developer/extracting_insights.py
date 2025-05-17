import json
import re
import time
import os
import google.generativeai as genai
from urllib.parse import urlparse

# Set up Gemini API
api_key = os.getenv("GOOGLE_API_KEY") or "AIzaSyBt5mm0ZFrH8cC6swW4XnxfyskScLEyfCs"
if not api_key:
    raise ValueError("API key not set.")
genai.configure(api_key=api_key)

# Extract date from article URL
def extract_date_from_url(url):
    match = re.search(r"/(\d{4})/(\d{2})/(\d{2})/", url)
    if match:
        return f"{match.group(1)}-{match.group(2)}-{match.group(3)}"
    return "Unknown"

# Clean and truncate content if too long
def clean_and_chunk_content(content, max_length=12000):
    content = content.strip().replace("\n", " ").replace("\\", "")
    return content[:max_length]

# Extraction prompt
base_prompt = prompt = f"""
You are an expert research analyst helping extract **structured insights** from technical articles. Your goal is to return detailed, meaningful fields in **flat JSON format** (no nesting), suitable for a Looker Studio dashboard.

Return ONLY valid JSON. Do NOT include explanations, markdown, or text outside the JSON. Do NOT omit available details.

Below are the detailed field-level instructions for extraction:

- "article_id": Unique identifier Hash number.
- "title": Use exact title from the article metadata.
- "link": Original article URL.
- "author": Comma-separated full names of the author(s). If unavailable, return "None".
- "publication_date": Extract in YYYY-MM-DD format using the article URL (e.g., /2024/12/01/ → 2024-12-01).
- "source": Publication name (e.g., TechCrunch, Wired).
- "summary": A clear 1-2 sentence summary covering the **main focus** or announcement.
- "keywords": Extract and prioritize top 3 relevant terms like technologies, companies, concepts (e.g., Generative AI, Series A, Mistral AI).
- "sentiment_score": Float value between -1 and 1. Analyze overall tone (e.g., optimistic growth → 0.7).
- "sentiment_percentage": Float 0-100 representing absolute sentiment intensity (e.g., strong positive = 85.0).
- "sentiment_analysis": One of: "Positive", "Neutral", or "Negative".
- "topics": Comma-separated high-level topics (e.g., Venture Capital, Data Privacy, Robotics).
- "people": Names of key individuals mentioned (e.g., Elon Musk, Sam Altman). If none, return "None".
- "organizations": List all companies/institutions mentioned (e.g., Nvidia, OpenAI, Y Combinator).
- "locations": City and Country where the news/initiative/event/product is based or relevant. Format: "San Francisco, USA". If none, return "None".
- "products": List the key products, platforms, services, or tools mentioned in the article. These can include newly launched products, existing market leaders, proprietary technologies, beta features, or services being shut down or acquired. Prioritize naming the actual product over just the company name. Example: 'ChatGPT, Claude 3, Gemini, Humane AI Pin, Microsoft Copilot'. If no products are clearly mentioned, return 'None'.
- "events": Major events or launches (e.g., CES 2024, Series A Funding). If none, return "None".
- "Business":  "Determine whether the article primarily discusses a B2B (business-to-business) or B2C (business-to-consumer) model. Base this on the nature of any mentioned products, innovations, services, or company offerings — and **who they are intended for**. If the target customers are businesses (e.g., SaaS tools, enterprise solutions), return 'B2B'. If the target customers are individual consumers (e.g., mobile apps, wearables, health platforms), return 'B2C'. If it's unclear or not mentioned, return 'None'.
- "funding_rounds": Comma-separated stages like "Pre-Seed, Series A". Only include valid rounds. If none, return "None".
- "investors": Names of VCs or investors (e.g., Sequoia Capital, a16z). Return "None" if not specified.
- "financial_metrics": Convert **all monetary figures** to float values in **Million USD** (e.g., $1.5B = 1500.0, $750K = 0.75). 
- "sectors":"Assign **one specific and standardized sector** from the following predefined list. Do not use vague or generic words like 'AI' or 'Technology'. Synonyms should be normalized (e.g., 'Health AI', 'AI in Healthcare' → 'Healthcare AI'). Select the best-fitting option from:

- Healthcare AI  
- Fintech AI  
- Agritech  
- Edtech  
- Retail Tech  
- Cybersecurity  
- Generative AI  
- Robotics & Automation  
- AI Governance  
- Digital Health  
- Legal Tech  
- AI in Marketing  
- Smart Mobility  
- AI in Real Estate  
- AI in Manufacturing  
- Climate Tech  
- Energy Tech  
- Space Tech  
- Construction Tech  
- Supply Chain & Logistics  
- Transportation Tech  
- Urban Tech  
- Cleantech  
- Food Tech  
- BioTech  
- Quantum Computing  
- Neuroscience Tech  
- Materials Science  
- Precision Medicine  
- Longevity Tech  
"
- "sub_sectors": More detailed sector level (e.g., Mental Health Platforms, RegTech, Supply Chain Robotics).
- "innovations": Comma-separated list of novel technologies, methods, business models, or product ideas mentioned in the article. These could be newly launched or proposed innovations, or even *implied* innovations based on trends and gaps. Highlight anything that could have the potential to become the next big breakthrough. Examples: 'Decentralized AI training on smartphones', 'Synthetic data generation for rare disease modeling', 'Zero-trust architecture for AI APIs'. Think beyond obvious features — include creative or disruptive ideas that might not yet be fully developed. Wrap Innovations in less words possible without any commas
- "trends": Comma-separated List of emerging and established trends mentioned or implied in the article. These should reflect what is gaining momentum in the market, particularly in AI and tech. Examples: 'Generative AI', 'AI Copilots', 'Real-time Compliance', 'AI-Powered Legal Tools'. Only include trends that are contextually relevant and visible in the article. Wrap Trends in less words possible without any commas
- "market_gaps": Comma-separated list of any clearly stated or implied market gaps in the article. These are areas where there is unmet demand, lack of innovation, underserved users, inefficient solutions, or emerging needs not yet addressed by current players. Market gaps can be inferred from problems discussed, limitations of current solutions, customer pain points, or missed opportunities. Examples include: 'Lack of privacy-first tools for Gen Z', 'No affordable AI-powered legal assistant for small firms', or 'Rural regions still lack access to AI-driven healthcare diagnostics'. Wrap market gaps in less words possible without any commas. If no meaningful market gaps are mentioned or implied, return 'None'.
- "competitor_analysis": Include comparisons or competitive mentions (e.g., "Anthropic competes with OpenAI").
- "customer_insights": Identify what consumers, developers, or businesses are struggling with, demanding, or reacting positively/negatively to. These are clues to what the market truly values or lacks. Examples: 'High demand for transparent AI decision-making', 'Startups prefer privacy-first AI models', 'Users are overwhelmed by data compliance complexity'.
- "relevance_score": Float 1-10 based on relevance to AI and emerging technology (10 = extremely relevant).
- "severity_score": Float 1-10 measuring the **impactfulness** or disruption potential of the content.

IMPORTANT:
- Do NOT skip any fields. If no value found, use "None" or 0 as appropriate.
- Follow all instructions carefully. Return a valid flat JSON object only.
- Focus on identifying innovations with disruptive potential — not just current solutions, but also ideas *hinted at* by market needs, pain points, and future demand.
- You are identifying opportunities for new innovation. Think like a startup founder scanning for:
    - Problems worth solving
    - Unmet customer needs
    - Gaps between current offerings and future demand
    - Patterns in emerging trends
    Your output should reflect strategic thinking beyond what is explicitly said.
- Wrap up market gaps, Trends, Innovations in less words possible without any commas in a single sentence.
"""

# Extract insights using Gemini
def extract_insights(article):
    url = article.get("url", "")
    content = article.get("content", "") or article.get("body", "")
    if not content:
        print(f"Skipping empty article: {url}")
        return {}

    article_id = url.split("/")[-1]
    publication_date = extract_date_from_url(url)
    source = urlparse(url).netloc.replace("www.", "").split(".")[0].capitalize()

    combined_prompt = base_prompt + f'\n\nArticle Metadata:\n' + json.dumps({
        "article_id": article_id,
        "title": article.get("title", ""),
        "link": url,
        "author": article.get("author", "None"),
        "publication_date": publication_date,
        "source": source
    }, indent=2)

    combined_prompt += f'\n\nArticle Content:\n"""\n{clean_and_chunk_content(content)}\n"""'

    time.sleep(2)  # Prevent API rate limiting

    try:
        model = genai.GenerativeModel("gemini-2.0-flash")
        response = model.generate_content(combined_prompt)
        raw = response.text.strip().replace("```json", "").replace("```", "")
        insights = json.loads(raw)
    except json.JSONDecodeError:
        print(f"[Error] JSON parse failed for article: {url}")
        insights = {}
    except Exception as e:
        print(f"[Error] {str(e)} for article: {url}")
        insights = {}

    return insights

# Load all articles
try:
    with open("all_techcrunch_articles_20250314.json", "r", encoding="utf-8") as f:
        articles = json.load(f)
except Exception as e:
    print(f"[File Error] {e}")
    articles = []

# Process each article
extracted_data = []
for i, article in enumerate(articles):
    print(f"Processing article {i+1}/{len(articles)}")
    result = extract_insights(article)
    if result:
        extracted_data.append(result)

# Save extracted insights
with open("extracted_insights4.json", "w", encoding="utf-8") as out:
    json.dump(extracted_data, out, indent=2, ensure_ascii=False)

print("✅ Extraction complete. Output saved to extracted_insights.json")
