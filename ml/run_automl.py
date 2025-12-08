#!/usr/bin/env python3
import argparse
import json
import os
import sys
from datetime import datetime, timezone
from typing import Optional, List, Tuple

import pandas as pd
from sklearn.model_selection import train_test_split

# Optional import: only needed when using --mongo-collection
try:
    from pymongo import MongoClient
except Exception:  # pragma: no cover
    MongoClient = None  # type: ignore

try:
    from supervised.automl import AutoML
except ImportError as e:
    print(
        "Error: mljar-supervised is not installed.\n"
        "Install dependencies first:\n"
        "  pip install mljar-supervised pandas scikit-learn pymongo",
        file=sys.stderr,
    )
    raise


def _timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")


def _ensure_results_path(path: Optional[str]) -> str:
    if path:
        rp = os.path.abspath(path)
    else:
        rp = os.path.abspath(os.path.join("ml", "outputs", _timestamp()))
    os.makedirs(rp, exist_ok=True)
    return rp


def _maybe_str_to_json(s: Optional[str]):
    if not s:
        return None
    try:
        return json.loads(s)
    except Exception:
        raise SystemExit(f"Invalid JSON provided: {s!r}")


def _load_from_csv(path: str) -> pd.DataFrame:
    p = os.path.abspath(path)
    if not os.path.isfile(p):
        raise SystemExit(f"CSV file not found: {p}")
    df = pd.read_csv(p)
    return df


def _connect_mongo(uri_env="MONGODB_URI") -> MongoClient:
    if MongoClient is None:
        raise SystemExit("pymongo is not installed. Please: pip install pymongo")
    uri = os.getenv(uri_env)
    if not uri:
        raise SystemExit(
            f"{uri_env} environment variable is not set. Please export your MongoDB connection string."
        )
    return MongoClient(uri)


def _load_from_mongo(
    collection: str,
    database: Optional[str] = None,
    query: Optional[dict] = None,
    projection: Optional[dict] = None,
    limit: Optional[int] = None,
) -> pd.DataFrame:
    client = _connect_mongo()
    # Default DB: try env MONGODB_DB, fallback to "TradingApp"
    db_name = database or os.getenv("MONGODB_DB") or "TradingApp"
    db = client[db_name]
    col = db[collection]
    cursor = col.find(query or {}, projection or {})
    if isinstance(limit, int) and limit > 0:
        cursor = cursor.limit(limit)
    rows = list(cursor)
    if not rows:
        raise SystemExit(
            f"No documents returned from MongoDB collection '{db_name}.{collection}' with query={query or {}}"
        )
    # Normalize ObjectId and nested structures if simple
    for r in rows:
        if "_id" in r:
            r["_id"] = str(r["_id"])
    df = pd.DataFrame(rows)
    return df


def _detect_task(y: pd.Series) -> str:
    # Let AutoML handle task detection by default, but we can provide a hint:
    # Return one of 'binary_classification', 'multiclass_classification', 'regression'
    try:
        nunique = int(y.nunique(dropna=True))
        if y.dtype.kind in "bOUS":  # boolean or string/categorical-like
            if nunique <= 2:
                return "binary_classification"
            return "multiclass_classification"
        # numeric
        if nunique <= 2:
            return "binary_classification"
        # Heuristic: many unique values => regression
        return "regression"
    except Exception:
        return "regression"


def _split_xy(df: pd.DataFrame, target: str) -> Tuple[pd.DataFrame, pd.Series]:
    if target not in df.columns:
        raise SystemExit(f"Target column '{target}' not found in data columns: {list(df.columns)}")
    y = df[target]
    X = df.drop(columns=[target])
    return X, y


def _comma_list(s: Optional[str]) -> Optional[List[str]]:
    if not s:
        return None
    out = [x.strip() for x in s.split(",")]
    return [x for x in out if x]


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Run AutoML with mljar-supervised on CSV or MongoDB data."
    )
    src = p.add_argument_group("Input")
    src.add_argument("--input-csv", help="Path to input CSV file.")
    src.add_argument(
        "--mongo-collection",
        help="MongoDB collection name to load data from (requires MONGODB_URI env).",
    )
    src.add_argument(
        "--mongo-db",
        help="MongoDB database name. Defaults to $MONGODB_DB or 'TradingApp' if not set.",
    )
    src.add_argument(
        "--mongo-query",
        help="MongoDB query as JSON string. Example: '{\"symbol\":\"AAPL\"}'",
    )
    src.add_argument(
        "--mongo-projection",
        help="MongoDB projection as JSON string. Example: '{\"_id\":0, \"close\":1, \"target\":1}'",
    )
    src.add_argument(
        "--mongo-limit",
        type=int,
        help="Limit documents loaded from MongoDB.",
    )
    src.add_argument(
        "--collection",
        help="Alias for --mongo-collection. If both provided, --mongo-collection wins.",
    )
    src.add_argument(
        "--symbol",
        help="If set, will add {'symbol': SYMBOL} to the MongoDB query (uppercased).",
    )

    p.add_argument("--target", default="ClosePrice", help="Target column name (default: ClosePrice).")
    p.add_argument(
        "--drop-columns",
        help="Comma-separated list of columns to drop before training.",
    )
    p.add_argument("--test-size", type=float, default=0.25, help="Test size fraction (default: 0.25)")
    p.add_argument(
        "--random-state",
        type=int,
        default=1337,
        help="Random seed for splitting and modeling (default: 1337).",
    )
    p.add_argument(
        "--mode",
        choices=["Explain", "Perform", "Compete"],
        default="Explain",
        help="AutoML mode (default: Explain).",
    )
    p.add_argument(
        "--time-limit",
        type=int,
        default=600,
        help="Total time limit in seconds for AutoML training (default: 600).",
    )
    p.add_argument(
        "--algorithms",
        help="Comma-separated list of algorithms to try "
        "(e.g. 'LightGBM,Xgboost,Random Forest'). Defaults to AutoML's selection.",
    )
    p.add_argument(
        "--eval-metric",
        help="Evaluation metric (e.g. 'logloss', 'auc', 'rmse'). Defaults to AutoML's choice.",
    )
    p.add_argument(
        "--explain-level",
        type=int,
        default=2,
        help="Explainability level (0-2). Higher = more plots (default: 2).",
    )
    p.add_argument(
        "--results-path",
        help="Directory to store AutoML outputs. Defaults to ml/outputs/<timestamp>.",
    )
    p.add_argument(
        "--no-stratify",
        action="store_true",
        help="Disable stratified split for classification tasks.",
    )
    p.add_argument(
        "--save-predictions",
        action="store_true",
        help="Save test predictions to CSV in results path.",
    )
    p.add_argument(
        "--verbose",
        type=int,
        default=1,
        help="Verbosity level for AutoML (default: 1).",
    )
    return p.parse_args(argv)


def load_dataframe(args: argparse.Namespace) -> pd.DataFrame:
    has_csv = bool(args.input_csv)
    has_mongo = bool(args.mongo_collection or args.collection)

    if has_csv == has_mongo:
        # Either both set or neither set
        raise SystemExit("Please specify exactly one data source: --input-csv OR --mongo-collection")

    if has_csv:
        df = _load_from_csv(args.input_csv)
        if args.symbol and "symbol" in df.columns:
            sym = str(args.symbol).strip().upper()
            df["symbol"] = df["symbol"].astype(str).str.upper()
            df = df[df["symbol"] == sym]
            if df.empty:
                raise SystemExit(f"No rows found in CSV after filtering for symbol '{sym}'")
    else:
        base_query = _maybe_str_to_json(args.mongo_query) or {}
        if args.symbol:
            base_query = {**base_query, "symbol": str(args.symbol).strip().upper()}
        projection = _maybe_str_to_json(args.mongo_projection)
        coll = args.mongo_collection or args.collection
        if not coll:
            raise SystemExit("Please provide --mongo-collection or --collection for MongoDB source")
        df = _load_from_mongo(
            collection=coll,
            database=args.mongo_db,
            query=base_query or None,
            projection=projection,
            limit=args.mongo_limit,
        )

    drops = _comma_list(args.drop_columns) or []
    if args.symbol and "symbol" in df.columns:
        drops = list(set(list(drops) + ["symbol"]))
    if drops:
        existing = [c for c in drops if c in df.columns]
        if existing:
            df = df.drop(columns=existing)
    return df


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    results_path = _ensure_results_path(args.results_path)

    df = load_dataframe(args)
    X, y = _split_xy(df, args.target)
    task_hint = _detect_task(y)

    # Stratify only for classification
    stratify = None
    if not args.no_stratify and "classification" in task_hint:
        try:
            stratify = y
        except Exception:
            stratify = None

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=args.test_size, random_state=args.random_state, stratify=stratify
    )

    algorithms = _comma_list(args.algorithms)

    automl_kwargs = dict(
        mode=args.mode,
        total_time_limit=args.time_limit,
        results_path=results_path,
        eval_metric=args.eval_metric,
        explain_level=args.explain_level,
        random_state=args.random_state,
        verbose=args.verbose,
        # Let AutoML detect task, but we provide a hint via 'ml_task' if desired.
        # ml_task=task_hint,
    )
    if algorithms:
        automl_kwargs["algorithms"] = algorithms

    automl = AutoML(**automl_kwargs)

    print(f"[mljar] Starting AutoML in mode={args.mode} time_limit={args.time_limit}s")
    print(f"[mljar] Results path: {results_path}")
    print(f"[mljar] Detected task hint: {task_hint}")

    automl.fit(X_train, y_train)

    # Evaluate on holdout
    preds = automl.predict(X_test)
    # AutoML has built-in scoring in reports; optionally compute basic score if numeric
    try:
        import numpy as np

        if "classification" in task_hint:
            from sklearn.metrics import accuracy_score

            # If prediction is probability array, take argmax
            if isinstance(preds, (list, tuple)) or getattr(preds, "ndim", 1) > 1:
                preds_labels = preds.argmax(axis=1)
            else:
                preds_labels = preds
            acc = accuracy_score(y_test, preds_labels)
            print(f"[mljar] Holdout accuracy: {acc:.4f}")
        else:
            from sklearn.metrics import r2_score

            r2 = r2_score(y_test, preds)
            print(f"[mljar] Holdout R^2: {r2:.4f}")
    except Exception as e:
        print(f"[mljar] Skipping quick metric computation due to: {e}")

    if args.save_predictions:
        out_csv = os.path.join(results_path, "test_predictions.csv")
        pd.DataFrame(
            {
                "index": getattr(y_test, "index", range(len(y_test))),
                "y_true": y_test.values,
                "y_pred": preds if hasattr(preds, "shape") and getattr(preds, "ndim", 1) == 1 else preds,
            }
        ).to_csv(out_csv, index=False)
        print(f"[mljar] Saved predictions to: {out_csv}")

    print("[mljar] AutoML completed.")
    print(f"[mljar] Explore reports and models in: {results_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
