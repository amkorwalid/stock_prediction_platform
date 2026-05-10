import Link from "next/link";

export default function Home() {
  return (
    <div className="min-h-screen px-6 py-8 md:px-10">
      <main className="mx-auto flex w-full max-w-6xl flex-col gap-8">
        <header className="panel p-6 md:p-8">
          <p className="mb-3 text-sm tracking-[0.2em] subtle">STOCK PREDICTION PLATFORM</p>
          <h1 className="text-3xl font-semibold md:text-5xl">
            AI-Powered Market Intelligence for Learning and Research
          </h1>
          <p className="mt-5 max-w-3xl text-base subtle md:text-lg">
            Explore stock data, model forecasts, and sentiment insights in a dashboard
            inspired by modern trading terminals.
          </p>
          <div className="mt-7 flex flex-wrap gap-3">
            <Link
              href="/dashboard"
              className="rounded-full bg-[var(--accent)] px-6 py-3 text-sm font-semibold text-slate-950 transition hover:opacity-90"
            >
              Open Dashboard
            </Link>
            <a
              href="http://localhost:8000/docs"
              target="_blank"
              rel="noreferrer"
              className="rounded-full border border-[var(--border)] px-6 py-3 text-sm font-semibold transition hover:bg-[var(--surface-soft)]"
            >
              API Docs
            </a>
          </div>
        </header>

        <section className="grid gap-4 md:grid-cols-3">
          <article className="panel p-5">
            <p className="text-sm subtle">Data Sources</p>
            <h2 className="mt-2 text-lg font-semibold">OHLCV + News</h2>
            <p className="mt-2 text-sm subtle">Structured market and headline data pipelines.</p>
          </article>
          <article className="panel p-5">
            <p className="text-sm subtle">ML Layer</p>
            <h2 className="mt-2 text-lg font-semibold">Regression + Classification</h2>
            <p className="mt-2 text-sm subtle">
              Multi-horizon predictions with confidence and direction probabilities.
            </p>
          </article>
          <article className="panel p-5">
            <p className="text-sm subtle">Visualization</p>
            <h2 className="mt-2 text-lg font-semibold">Trading-Style Dashboard</h2>
            <p className="mt-2 text-sm subtle">Watchlist, model outputs, and sentiment snapshots.</p>
          </article>
        </section>

        <section className="panel border-[color:var(--negative)] bg-[linear-gradient(180deg,#231222_0%,#1a1024_100%)] p-5">
          <p className="text-xs font-semibold tracking-[0.18em] text-[var(--negative)]">IMPORTANT</p>
          <p className="mt-2 text-sm md:text-base">
            This project is for <strong>educational purposes only</strong>. It is not financial
            advice, not a brokerage product, and not intended for live trading decisions.
          </p>
        </section>
      </main>
    </div>
  );
}
        </div>
        <div className="flex flex-col gap-4 text-base font-medium sm:flex-row">
          <a
            className="flex h-12 w-full items-center justify-center gap-2 rounded-full bg-foreground px-5 text-background transition-colors hover:bg-[#383838] dark:hover:bg-[#ccc] md:w-[158px]"
            href="https://vercel.com/new?utm_source=create-next-app&utm_medium=appdir-template-tw&utm_campaign=create-next-app"
            target="_blank"
            rel="noopener noreferrer"
          >
            <Image
              className="dark:invert"
              src="/vercel.svg"
              alt="Vercel logomark"
              width={16}
              height={16}
            />
            Deploy Now
          </a>
          <a
            className="flex h-12 w-full items-center justify-center rounded-full border border-solid border-black/[.08] px-5 transition-colors hover:border-transparent hover:bg-black/[.04] dark:border-white/[.145] dark:hover:bg-[#1a1a1a] md:w-[158px]"
            href="https://nextjs.org/docs?utm_source=create-next-app&utm_medium=appdir-template-tw&utm_campaign=create-next-app"
            target="_blank"
            rel="noopener noreferrer"
          >
            Documentation
          </a>
        </div>
      </main>
    </div>
  );
}
