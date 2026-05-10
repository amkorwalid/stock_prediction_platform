## Getting Started

Install dependencies and run the development server:

```bash
npm ci
npm run dev
```

Open [http://localhost:3000](http://localhost:3000) with your browser to see the result.

## API configuration

The frontend expects the backend API under:

- `NEXT_PUBLIC_API_BASE_URL` (default: `http://localhost:8000/api`)

Example:

```bash
NEXT_PUBLIC_API_BASE_URL=http://localhost:8000/api npm run dev
```

## Pages

- `/` — Landing page with project overview and educational-purpose disclaimer
- `/dashboard` — Trading-style dashboard connected to backend API endpoints
