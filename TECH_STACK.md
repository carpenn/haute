# Runway — Tech Stack

Reference implementation: [PricingFrontier/atelier](https://github.com/PricingFrontier/atelier)

## Architecture

```
┌─────────────────────────────────────┐
│   Browser (React + TypeScript)      │
│  ┌──────────┬──────────┬──────────┐ │
│  │          │          │          │ │
│  │  Panel   │  Panel   │  Panel   │ │
│  │          │          │          │ │
│  └──────────┴──────────┴──────────┘ │
│         REST + WebSocket            │
└──────────────┬──────────────────────┘
               │
┌──────────────┴──────────────────────┐
│  Python backend (FastAPI + uvicorn) │
│  ┌────────────────────────────────┐ │
│  │   rustystats (Rust engine)     │ │
│  └────────────────────────────────┘ │
│  Storage: SQLite                    │
└─────────────────────────────────────┘
```

Single Python package — the React frontend is pre-built and bundled as static assets inside the wheel. No Node.js runtime required in production.

---

## Backend — Python

| Concern | Library | Version |
|---|---|---|
| **Language** | Python | >= 3.13 |
| **Package manager** | uv | (with `uv.lock`) |
| **Build system** | Hatchling | — |
| **Web framework** | FastAPI | >= 0.115.0 |
| **ASGI server** | uvicorn\[standard\] | >= 0.34.0 |
| **WebSockets** | websockets | >= 14.0 |
| **Validation / settings** | Pydantic | >= 2.10.0 |
| **ORM / SQL** | SQLAlchemy | >= 2.0.36 |
| **Async SQLite driver** | aiosqlite | >= 0.20.0 |
| **DataFrames** | Polars | >= 1.0.0 |
| **CLI** | Click | >= 8.1.0 |
| **Multipart forms** | python-multipart | >= 0.0.18 |
| **Compute engine** | rustystats (Rust via PyO3) | >= 0.4.10 |

### Dev dependencies

| Library | Purpose |
|---|---|
| pytest (>= 9.0) | Test runner |
| pytest-asyncio | Async test support |
| httpx | Async HTTP test client |

---

## Frontend — TypeScript / React

| Concern | Library | Version |
|---|---|---|
| **Language** | TypeScript | ~5.9 |
| **UI framework** | React | ^19.2 |
| **Routing** | react-router-dom | ^7.13 |
| **Bundler / dev server** | Vite | ^8.0 (beta) |
| **Vite React plugin** | @vitejs/plugin-react | ^5.1 |
| **Styling** | Tailwind CSS v4 | ^4.1 |
| **Tailwind Vite plugin** | @tailwindcss/vite | ^4.1 |
| **Class utilities** | clsx + tailwind-merge + class-variance-authority | — |
| **Icons** | lucide-react | ^0.563 |
| **Charts** | Recharts | ^3.7 |

### Dev tooling

| Tool | Version |
|---|---|
| ESLint | ^9.39 |
| eslint-plugin-react-hooks | ^7.0 |
| eslint-plugin-react-refresh | ^0.4 |
| typescript-eslint | ^8.48 |

### Vite config highlights

- `@` path alias → `./src`
- Dev proxy: `/api` → backend, `/ws` → backend (WebSocket)
- Production build output → `../src/atelier/static/` (bundled into the Python wheel)

---

## Project layout (atelier reference)

```
.
├── frontend/          # React + TypeScript (Vite)
├── src/atelier/       # Python package (FastAPI)
│   └── static/        # Built frontend assets (generated)
├── tests/             # pytest
├── analysis/          # Analysis notebooks / scripts
├── docs/              # Documentation
├── pyproject.toml     # Python project config (hatchling)
├── uv.lock            # Lockfile
└── .python-version    # 3.13
```

---

## Key patterns

- **Monorepo** — backend and frontend live in the same repository
- **uv** for Python dependency management and virtual environments
- **Hatchling** as the Python build backend
- **Polars** over pandas for DataFrame operations
- **Pydantic v2** for data validation and API schemas
- **SQLAlchemy + aiosqlite** for async SQLite persistence
- **WebSockets** for real-time progress updates from backend to frontend
- **Tailwind CSS v4** with the Vite plugin (no PostCSS config needed)
- **Vite dev proxy** to avoid CORS during local development
- **Static bundling** — frontend builds into the Python package for single-artifact distribution
