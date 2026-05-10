import Link from "next/link";

import {
  getLatestNews,
  getLatestPrediction,
  getLatestSentiment,
  getStocks,
} from "@/lib/api";

const DEFAULT_SYMBOL = "AAPL";

function sentimentColor(label?: string) {
  if (label === "Positive") return "text-[var(--positive)]";
  if (label === "Negative") return "text-[var(--negative)]";
  return "text-yellow-300";
}

type DashboardPageProps = {
  searchParams?: Promise<{ symbol?: string }>;
};

export default async function DashboardPage({ searchParams }: DashboardPageProps) {
  const stocks = await getStocks(8);
  const resolvedSearchParams = searchParams ? await searchParams : undefined;
  const requestedSymbol = resolvedSearchParams?.symbol?.toUpperCase();
  const selectedSymbol = requestedSymbol
    ? stocks.find((stock) => stock.symbol === requestedSymbol)?.symbol
    : undefined;
  const activeSymbol = selectedSymbol ?? stocks[0]?.symbol ?? DEFAULT_SYMBOL;

  const [prediction, news, sentiment] = await Promise.all([
    getLatestPrediction(activeSymbol),
    getLatestNews(activeSymbol, 6),
    getLatestSentiment(activeSymbol),
  ]);

  const regression = prediction ? Object.entries(prediction.regression) : [];
  const classification = prediction ? Object.entries(prediction.classification) : [];

  return (
    <div className="min-h-screen px-6 py-8 md:px-10">
      <main className="mx-auto flex w-full max-w-7xl flex-col gap-6">
        <header className="flex flex-wrap items-center justify-between gap-3">
          <div>
            <p className="text-xs tracking-[0.18em] subtle">DASHBOARD</p>
            <h1 className="text-2xl font-semibold md:text-3xl">
              Market Intelligence Terminal · {activeSymbol}
            </h1>
          </div>
          <Link href="/" className="rounded-full border border-[var(--border)] px-4 py-2 text-sm">
            Back to Landing
          </Link>
        </header>

        <section className="grid gap-4 lg:grid-cols-4">
          {stocks.map((stock) => (
            <article key={stock.symbol} className="panel p-4">
              <p className="text-xs subtle">{stock.exchange}</p>
              <Link
                href={`/dashboard?symbol=${encodeURIComponent(stock.symbol)}`}
                className={`mt-1 block text-lg font-semibold ${stock.symbol === activeSymbol ? "text-[var(--accent)]" : ""}`}
              >
                {stock.symbol}
              </Link>
              <p className="mt-1 line-clamp-1 text-sm subtle">{stock.company}</p>
            </article>
          ))}
          {stocks.length === 0 && (
            <article className="panel p-4 lg:col-span-4">
              <p className="text-sm subtle">
                Unable to load watchlist from API. Check backend URL and availability.
              </p>
            </article>
          )}
        </section>

        <section className="grid gap-4 xl:grid-cols-3">
          <article className="panel p-5 xl:col-span-2">
            <h2 className="text-lg font-semibold">Latest Model Forecasts</h2>
            <div className="mt-4 grid gap-3 md:grid-cols-2">
              <div>
                <p className="mb-2 text-sm subtle">Regression (Expected Return)</p>
                {regression.length ? (
                  <ul className="space-y-2">
                    {regression.map(([horizon, row]) => (
                      <li
                        key={horizon}
                        className="flex items-center justify-between rounded-xl border border-[var(--border)] px-3 py-2"
                      >
                        <span className="text-sm subtle">{horizon}</span>
                        <span
                          className={`text-sm font-semibold ${row.pred_return >= 0 ? "text-[var(--positive)]" : "text-[var(--negative)]"}`}
                        >
                          {(row.pred_return * 100).toFixed(2)}%
                        </span>
                      </li>
                    ))}
                  </ul>
                ) : (
                  <p className="text-sm subtle">No regression forecasts available.</p>
                )}
              </div>

              <div>
                <p className="mb-2 text-sm subtle">Classification (Direction Probabilities)</p>
                {classification.length ? (
                  <ul className="space-y-2">
                    {classification.map(([horizon, row]) => (
                      <li
                        key={horizon}
                        className="rounded-xl border border-[var(--border)] px-3 py-2"
                      >
                        <p className="text-sm subtle">{horizon}</p>
                        <p className="mt-1 text-xs">
                          Up {(row.prob_up * 100).toFixed(1)}% · Flat{" "}
                          {(row.prob_flat * 100).toFixed(1)}% · Down{" "}
                          {(row.prob_down * 100).toFixed(1)}%
                        </p>
                      </li>
                    ))}
                  </ul>
                ) : (
                  <p className="text-sm subtle">No classification forecasts available.</p>
                )}
              </div>
            </div>
          </article>

          <article className="panel p-5">
            <h2 className="text-lg font-semibold">Sentiment Signal</h2>
            {sentiment ? (
              <div className="mt-4 space-y-2">
                <p className={`text-xl font-semibold ${sentimentColor(sentiment.sentiment_label)}`}>
                  {sentiment.sentiment_label}
                </p>
                <p className="text-sm subtle">Score: {sentiment.sentiment_score.toFixed(3)}</p>
                <p className="text-sm subtle">Articles: {sentiment.article_count}</p>
                <p className="text-sm subtle">Trade Date: {sentiment.trade_date}</p>
              </div>
            ) : (
              <p className="mt-4 text-sm subtle">No sentiment signal found for this symbol.</p>
            )}
          </article>
        </section>

        <section className="panel p-5">
          <h2 className="text-lg font-semibold">Market News Feed</h2>
          {news.length ? (
            <ul className="mt-4 space-y-3">
              {news.map((item) => (
                <li
                  key={item.article_id}
                  className="rounded-xl border border-[var(--border)] px-4 py-3"
                >
                  <p className="text-xs subtle">
                    {item.symbol} · {item.source} · {new Date(item.published_at).toLocaleString()}
                  </p>
                  <p className="mt-1 text-sm">{item.headline}</p>
                </li>
              ))}
            </ul>
          ) : (
            <p className="mt-3 text-sm subtle">No news available for {activeSymbol}.</p>
          )}
        </section>
      </main>
    </div>
  );
}
