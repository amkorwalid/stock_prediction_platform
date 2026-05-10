workspace "Stock Prediction Platform" {

    model {

        # ── Actors ──────────────────────────────────────────────
        user = person "Trader & Investor" {
            description "Views price and movement predictions."
        }

        # ── Internal system ─────────────────────────────────────
        system = softwareSystem "Stock Prediction Platform" {
            description "Aggregates market data and news, runs ML prediction models, and surfaces results to users."

            # — Web Server (Public IP) —
            webapp = group "Web Server (Public IP)" {

                nginx = container "Nginx" {
                    description "Routes /api/* to FastAPI and all other paths to Next.js."
                    technology "Nginx 1.25"
                    tags "Proxy"
                }

                frontend = container "Frontend" {
                    description "Server-rendered React application that displays dashboards, predictions, and news analysis."
                    technology "Next.js · React.js · Port 3000"
                    tags "WebApp"

                    homepage  = component "Homepage"  "Marketing landing page."                                               "Next.js Page"
                    dashboard = component "Dashboard"  "Price charts, prediction widgets, and news sentiment cards." "Next.js Page"
                    layout    = component "Layout"     "Shared UIs components: header, sidebar navigation, and footer."                "Next.js Component"
                }

                backend = container "Backend API" {
                    description "Stateless REST service exposing stock, news, prediction, and analysis endpoints."
                    technology "Python 3 · FastAPI · Uvicorn · Port 8000"
                    tags "API"
                    url "http://localhost:8000/docs"

                    api      = component "API Layer"     "Routes: /stocks  /news  /predictions  /news-analysis" "FastAPI Router"
                    services = component "Service Layer"  "StockService  NewsService  PredictionService  AnalysisService" "Python Classes"
                    models   = component "Data Models"   "SQLAlchemy ORM entities and Pydantic request/response schemas." "SQLAlchemy / Pydantic"
                }
            }

            # — Database Server (Private IP) —
            database = group "Database Server (Private IP)" {

                postgres = container "Database" {
                    description "Central data warehouse storing OHLCV time-series, news articles, ML predictions, audit logs, and metadata."
                    technology "PostgreSQL · Port 5432"
                    tags "Database"
                }
            }

            # — Pipeline Server (Private IP) —
            data_pipeline = group "Pipeline Server (Private IP)" {

                airflow = container "Apache Airflow" {
                    description "Orchestrates and monitors all ETL and ML training DAGs on a schedule."
                    technology "Apache Airflow 2.9 · CeleryExecutor"
                    tags "Orchestrator"

                    dags    = component "DAGs"    "Defines pipeline schedules and task dependencies." "Python DAG"
                    plugins = component "Plugins"  "Custom operators, sensors, and hooks for stock and news tasks." "Airflow Plugin"
                }

                scripts = container "Python Scripts" {
                    description "Standalone ETL and ML scripts executed by Airflow tasks."
                    technology "Python 3.11 · pandas · scikit-learn"
                    tags "Worker"

                    data_fetching  = component "Data Fetching"  "Pulls raw OHLCV data and news articles from external APIs." "Python Module"
                    preprocessing  = component "Preprocessing"   "Cleans, transforms, and enriches data; runs news sentiment via LLMs." "Python Module"
                    model_training = component "Model Training"  "Trains ML models and persists prediction results." "Python Module"
                }
            }
        }

        # ── External systems ─────────────────────────────────────
        yahoo_finance = softwareSystem "Yahoo Finance" {
            description "Provides end-of-day and intraday OHLCV market data via REST API."
            tags "External"
        }

        news_api = softwareSystem "Alpha Vantage" {
            description "Provides financial news and company-specific articales."
            tags "External"
        }

        openai = softwareSystem "LLMs API" {
            description "LLMs used for news sentiment classification and summarisation."
            tags "External"
        }

        # ── Relationships ─────────────────────────────────────────

        # L1 — System context
        user   -> system        "Views dashboards and predictions"
        system -> yahoo_finance "Fetches OHLCV data"              "HTTPS/REST"
        system -> news_api      "Fetches financial news"          "HTTPS/REST"
        system -> openai        "Analyses news sentiment"         "HTTPS/REST"

        # L2 — Container
        user     -> nginx    "Accesses via browser"            "HTTPS"
        nginx    -> frontend "Forwards page requests"          "HTTP"
        nginx    -> backend  "Forwards /api/* requests"        "HTTP"
        frontend -> backend  "Fetches data for UI"             "REST/JSON"
        backend  -> postgres "Reads and writes records"        "SQL / SQLAlchemy"

        airflow  -> scripts  "Triggers ETL and training tasks" "Subprocess / Operator"
        airflow  -> postgres "Reads and writes DAG state and run logs" "SQL / psycopg2"

        scripts -> postgres      "Reads raw data; writes results"  "SQL / psycopg2"
        scripts -> yahoo_finance "Fetches market data"             "HTTPS/REST"
        scripts -> news_api      "Fetches news articles"           "HTTPS/REST"
        scripts -> openai        "Sends news for analysis"         "HTTPS/REST"

        # L3 — Backend components
        api      -> services "Process business logic"             "Python call"
        services -> models   "Reads and constructs domain objects"  "Python call"
        models   -> postgres "Persists and queries entities"        "SQLAlchemy ORM"

        # L3 — Frontend components
        dashboard -> backend "Fetches predictions, news, and charts" "REST/JSON"

        # L3 — Airflow components
        dags    -> plugins  "Uses custom operators and sensors"  "Airflow task"
        plugins -> scripts  "Invokes script modules"             "Python subprocess"

        # L3 — Script components
        data_fetching  -> yahoo_finance "Pulls OHLCV data"                   "HTTPS/REST"
        data_fetching  -> news_api      "Pulls news articles"                "HTTPS/REST"
        data_fetching  -> postgres      "Writes raw ingested records"        "SQL"
        preprocessing  -> openai        "Sends article text for analysis"    "HTTPS/REST"
        preprocessing  -> postgres      "Writes enriched data"               "SQL"
        model_training -> postgres      "Reads features; writes predictions" "SQL"

    } # ← closes model

    # ── Views ─────────────────────────────────────────────────────
    views {

        systemContext system "SystemContext" {
            include *
            autoLayout tb 300 150
            title "System Context — Stock Prediction Platform"
            description "Who uses the system and which external services it depends on."
        }

        container system "Containers" {
            include *
            autoLayout tb 250 150
            title "Container Diagram — Stock Prediction Platform"
            description "Internal containers grouped by network zone."
        }

        component frontend "Frontend_Components" {
            include *
            autoLayout tb
            title "Component Diagram — Frontend (Next.js)"
        }

        component backend "Backend_Components" {
            include *
            autoLayout tb
            title "Component Diagram — Backend API (FastAPI)"
        }

        component airflow "Airflow_Components" {
            include *
            autoLayout lr
            title "Component Diagram — Apache Airflow"
        }

        component scripts "Scripts_Components" {
            include *
            autoLayout lr
            title "Component Diagram — Python Scripts"
        }

        styles {

            element "Person" {
                shape Person
                background #1D9E75
                color #ffffff
            }

            element "WebApp" {
                background #378ADD
                color #ffffff
            }

            element "API" {
                background #185FA5
                color #ffffff
            }

            element "Proxy" {
                shape Pipe
                background #7F77DD
                color #ffffff
            }

            element "Database" {
                shape Cylinder
                background #D85A30
                color #ffffff
            }

            element "Orchestrator" {
                background #BA7517
                color #ffffff
            }

            element "Worker" {
                background #3B6D11
                color #ffffff
            }

            element "External" {
                background #888780
                color #ffffff
            }

            element "Software System" {
                background #378ADD
                color #ffffff
            }

            relationship "Relationship" {
                thickness 2
                color #888780
                style dashed
            }
        }

        theme default
    }
}