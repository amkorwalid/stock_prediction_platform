export type Ticker = {
  symbol: string;
  company: string;
  exchange: string;
  sector: string | null;
};

type PaginatedTickers = {
  items: Ticker[];
};

export type NewsArticle = {
  article_id: string;
  symbol: string;
  headline: string;
  source: string;
  published_at: string;
};

type PaginatedNews = {
  items: NewsArticle[];
};

export type SentimentRow = {
  symbol: string;
  sentiment_label: "Positive" | "Neutral" | "Negative";
  sentiment_score: number;
  article_count: number;
  trade_date: string;
};

type LatestRegressionPrediction = {
  pred_return: number;
  pred_close: number;
};

type LatestClassificationPrediction = {
  prob_up: number;
  prob_flat: number;
  prob_down: number;
};

export type LatestPrediction = {
  symbol: string;
  regression: Record<string, LatestRegressionPrediction>;
  classification: Record<string, LatestClassificationPrediction>;
};

export const API_BASE =
  process.env.NEXT_PUBLIC_API_BASE_URL ?? "http://localhost:8000/api";

export function getApiDocsUrl(): string {
  if (API_BASE.endsWith("/api/")) {
    return `${API_BASE.slice(0, -5)}/docs`;
  }
  if (API_BASE.endsWith("/api")) {
    return `${API_BASE.slice(0, -4)}/docs`;
  }
  return `${API_BASE}/docs`;
}

async function request<T>(path: string): Promise<T | null> {
  try {
    const response = await fetch(`${API_BASE}${path}`, {
      cache: "no-store",
      headers: { Accept: "application/json" },
    });

    if (!response.ok) {
      return null;
    }

    return (await response.json()) as T;
  } catch {
    return null;
  }
}

export async function getStocks(limit = 8): Promise<Ticker[]> {
  const payload = await request<PaginatedTickers>(`/stocks?limit=${limit}&offset=0`);
  return payload?.items ?? [];
}

export async function getLatestPrediction(symbol: string): Promise<LatestPrediction | null> {
  return request<LatestPrediction>(`/predictions/${encodeURIComponent(symbol)}/latest`);
}

export async function getLatestNews(symbol?: string, limit = 6): Promise<NewsArticle[]> {
  const query = symbol ? `?symbol=${encodeURIComponent(symbol)}&limit=${limit}` : `?limit=${limit}`;
  const payload = await request<PaginatedNews>(`/news${query}`);
  return payload?.items ?? [];
}

export async function getLatestSentiment(symbol?: string): Promise<SentimentRow | null> {
  const query = symbol ? `?symbol=${encodeURIComponent(symbol)}` : "";
  const payload = await request<SentimentRow[]>(`/news-analysis/latest${query}`);
  return payload?.[0] ?? null;
}
