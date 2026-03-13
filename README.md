# AI Holy Mother Han (AI 聖母韓鶴子)

AI chatbot providing guidance based on True Parents' teachings using RAG (Retrieval-Augmented Generation).

## Features

- **Multilingual Support**: Japanese, Korean, English
- **RAG System**: Searches 615 volumes of True Parents' Word Collection
- **Conversational AI**: Handles both theological and general life questions
- **Citation System**: Proper bibliographic references with volume numbers

## Architecture

- **Frontend**: React deployed at https://aiholymotherhan.com
- **Backend**: Flask API with OpenAI GPT-4o
- **Database**: 615 translated PDF texts (371 volumes completed)
- **Deployment**: AWS Elastic Beanstalk

## Files

- `application.py` - Flask API server (AWS EB entry point)
- `query_handler.py` - RAG query processing and response generation
- `theological_glossary.json` - Theological terminology mapping (Korean/English/Japanese)
- `requirements.txt` - Python dependencies
- `speeches/` - 615 volumes of True Parents' Word Collection (text files)
- `.ebextensions/` - AWS Elastic Beanstalk configuration

## Deployment

### Prerequisites

- AWS CLI configured
- EB CLI installed: `pip install awsebcli`
- OpenAI API key

### Deploy to AWS

```bash
# Initialize EB application
eb init -p python-3.11 ai-holy-mother-han-api --region ap-northeast-1

# Create environment and deploy
eb create ai-holy-mother-han-env \
  --envvars OPENAI_API_KEY=your-key-here \
  --instance-type t3.small

# For updates
eb deploy
```

### Environment Variables

Required in AWS EB:
- `OPENAI_API_KEY` - OpenAI API key for GPT-4o

## API Endpoints

- `POST /api/query` - Handle user queries
  ```json
  {
    "query": "嫌いな上司との関係を乗り越える方法を知りたい",
    "language": "ja"
  }
  ```

- `POST /api/search` - Search teachings
- `GET /api/citations` - Get citation index
- `GET /health` - Health check

## Local Development

```bash
# Install dependencies
pip install -r requirements.txt

# Set environment variable
export OPENAI_API_KEY=your-key-here

# Run server
python application.py
```

Server runs on http://localhost:5000

## Project Background

This project translates and indexes Rev. Sun Myung Moon's teachings (615 volumes) to provide AI-powered guidance combining:
- Theological knowledge from Divine Principle
- Practical life advice
- Relevant quotes from True Parents' teachings

## License

Private - True Parents' teachings
