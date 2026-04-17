# Real-Time Recommendation System

A full-stack recommendation system with ML-powered ranking, A/B testing, AI-powered search, and real-time event tracking.

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Backend API | FastAPI + Uvicorn |
| Database | MongoDB (Motor async driver) |
| ML Model | XGBoost Ranker + TF-IDF embeddings |
| A/B Testing | SHA-256 deterministic bucketing + MongoDB persistence |
| Search | MongoDB full-text + intent-based AI search |
| Frontend | React 18 + TailwindCSS + Radix UI |

---

## Prerequisites

- Python 3.10+
- Node.js 18+ / Yarn 1.x
- MongoDB 5+ (or Docker)

---

## Quick Start

### 1. Start MongoDB

**Option A — Docker (easiest):**
```bash
docker run -d --name mongo -p 27017:27017 mongo:7
```

**Option B — Local:** Follow [MongoDB Community Edition install guide](https://www.mongodb.com/docs/manual/administration/install-community/).

---

### 2. Backend Setup

```bash
cd backend

# Create and activate virtual environment
python -m venv .venv
.venv\Scripts\activate        # Windows
# source .venv/bin/activate   # macOS / Linux

# Install dependencies
pip install -r requirements.txt

# Configure environment
# Edit backend/.env — set ADMIN_API_KEY to a strong random string

# Start the server (auto-seeds data on first run)
uvicorn server:app --reload --port 8000
```

The API will be available at `http://localhost:8000`. Interactive docs at `http://localhost:8000/docs`.

---

### 3. Frontend Setup

```bash
cd frontend

# Install dependencies
yarn install

# Configure environment
# Edit frontend/.env — set REACT_APP_BACKEND_URL if your backend is on a different host

# Start dev server
yarn start
```

The frontend will open at `http://localhost:3000`.

---

## Environment Variables

### Backend (`backend/.env`)

| Variable | Default | Description |
|----------|---------|-------------|
| `MONGO_URL` | `mongodb://localhost:27017` | MongoDB connection string |
| `DB_NAME` | `recommendation_system` | Database name |
| `CORS_ORIGINS` | `http://localhost:3000,...` | Comma-separated allowed origins |
| `SEED_ON_STARTUP` | `true` | Seed sample data if DB is empty |
| `ADMIN_API_KEY` | *(none — endpoint disabled)* | Key required for `/api/admin/retrain` |
| `MODEL_DIR` | `./models` | Where to save/load the trained XGBoost model |

### Frontend (`frontend/.env`)

| Variable | Default | Description |
|----------|---------|-------------|
| `REACT_APP_BACKEND_URL` | `http://127.0.0.1:8000` | Backend base URL |
| `WDS_SOCKET_PORT` | `443` | Webpack dev server socket port |

---

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/health` | Health check |
| GET | `/api/recommend` | Personalized recommendations |
| GET | `/api/search` | Simple or AI-powered search |
| POST | `/api/search/ai` | AI natural-language search |
| POST | `/api/event` | Log a user interaction |
| GET | `/api/ab/arm` | Get A/B test bucket assignment |
| GET | `/api/popular` | Popular/trending items |
| GET | `/api/stats` | System statistics |
| GET | `/api/categories` | Available categories |
| GET | `/api/user/{user_id}/profile` | User profile + history |
| GET | `/api/item/{item_id}` | Item details + similar items |
| GET | `/api/experiments` | A/B experiment configurations |
| POST | `/api/admin/retrain` | Retrain ML model (requires `X-Admin-Key` header) |

---

## Running Tests

```bash
# API integration tests (requires server running)
python backend_test.py

# (Unit tests in tests/ — to be expanded)
cd backend && pytest tests/
```

---

## Architecture

```
frontend/ (React)
  └── App.js              Main application shell
  └── components/
       ├── Navigation         Header + user switcher
       ├── SearchBar          Simple + AI voice search
       ├── RecommendationCard Content card with actions
       ├── ItemModal          Detail view + interaction logging
       ├── StatsDashboard     Live system analytics
       ├── LoadingSpinner     Loading states
       └── ErrorBoundary      Global error recovery

backend/
  ├── server.py              FastAPI app + all REST endpoints
  ├── models.py              Pydantic data models
  ├── database.py            MongoDB singleton + queries
  ├── recommendation_engine  XGBoost ranking + TF-IDF embeddings
  ├── ab_testing.py          A/B experiment manager
  ├── search_engine.py       Intent-based search
  └── data_seeder.py         Sample data generator
```
