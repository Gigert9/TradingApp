#!/usr/bin/env python3
import argparse
import json
import os
import sys
from datetime import datetime, timezone
from typing import Optional, List, Tuple

import pandas as pd
import numpy as np
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
    y = df[target].squeeze()
    X = df.drop(columns=[target])
    return X, y


def _comma_list(s: Optional[str]) -> Optional[List[str]]:
    if not s:
        return None
    out = [x.strip() for x in s.split(",")]
    return [x for x in out if x]


def _parse_int_list(s: Optional[str]) -> List[int]:
    if not s:
        return []
    out: List[int] = []
    for tok in str(s).split(","):
        tok = tok.strip()
        if not tok:
            continue
        try:
            n = int(tok)
            if n > 0:
                out.append(n)
        except Exception:
            continue
    # unique and sorted
    return sorted(set(out))


def _featurize(
    df: pd.DataFrame,
    target: str,
    time_col: Optional[str],
    lags: List[int],
    windows: List[int],
) -> pd.DataFrame:
    # Add simple time-derived features if time column is present and datetime-like
    if time_col and time_col in df.columns:
        try:
            if not pd.api.types.is_datetime64_any_dtype(df[time_col]):
                df[time_col] = pd.to_datetime(df[time_col], utc=True, errors="coerce")
        except Exception:
            pass
        if pd.api.types.is_datetime64_any_dtype(df[time_col]):
            df["dow"] = df[time_col].dt.dayofweek
            df["hour"] = df[time_col].dt.hour

    # Choose a small set of base numeric columns to avoid feature explosion
    base_candidates = [
        target,
        "ClosePrice",
        "Close",
        "close",
        "Adj Close",
        "adj_close",
        "VWAP",
        "vwap",
        "Volume",
        "volume",
        "Open",
        "High",
        "Low",
    ]
    base_cols = [c for c in base_candidates if c in df.columns]
    if target in df.columns and target not in base_cols:
        base_cols.insert(0, target)

    # Add lag features
    for col in base_cols:
        for L in lags:
            try:
                df[f"{col}_lag{L}"] = df[col].shift(L)
            except Exception:
                pass

    # Add rolling mean/std features
    for col in base_cols:
        for W in windows:
            try:
                df[f"{col}_rollmean{W}"] = df[col].rolling(W).mean()
                df[f"{col}_rollstd{W}"] = df[col].rolling(W).std()
            except Exception:
                pass

    return df


def _apply_target_transform(kind: str, y: pd.Series, base: pd.Series) -> pd.Series:
    kind = (kind or "none").lower()
    if kind == "pct":
        return (y.astype(float) / base.astype(float)) - 1.0
    if kind == "log":
        return np.log(y.astype(float) / base.astype(float))
    return y


def _invert_target_transform(kind: str, base: pd.Series, yhat) -> pd.Series:
    kind = (kind or "none").lower()
    yhat_s = pd.Series(yhat).astype(float)
    base_s = pd.Series(base).astype(float)
    if kind == "pct":
        return base_s * (1.0 + yhat_s)
    if kind == "log":
        return base_s * np.exp(yhat_s)
    return yhat_s


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
    p.add_argument(
        "--time-column",
        default="t",
        help="Name of the time column to use as expectedtime (default: t). If not found, will try common names like 'Timestamp'. The time column is removed from features.",
    )
    p.add_argument(
        "--output-collection",
        default="prediction",
        help="MongoDB collection name to write predictions to (default: prediction).",
    )
    p.add_argument(
        "--predict-next",
        action="store_true",
        help="Predict ClosePrice at t+1 from features at t and save a single next-step forecast.",
    )
    p.add_argument(
        "--tune-strong",
        action="store_true",
        help="Enable stronger tuning (switch to 'Compete' mode if currently 'Explain').",
    )
    p.add_argument(
        "--validation-type",
        choices=["auto", "split", "kfold", "time"],
        default="auto",
        help="Validation strategy. 'auto' picks 'time' if time column present or --predict-next, otherwise 'split'.",
    )
    p.add_argument(
        "--train-ratio",
        type=float,
        default=0.8,
        help="Train ratio for split/time validation (default: 0.8).",
    )
    p.add_argument(
        "--k-folds",
        type=int,
        default=5,
        help="Number of folds for kfold validation (default: 5).",
    )
    p.add_argument(
        "--lags",
        default="1,2,3",
        help="Comma-separated positive integers for lag features to add (default: 1,2,3).",
    )
    p.add_argument(
        "--rolling-windows",
        default="5,10",
        help="Comma-separated positive integers for rolling window features (mean/std) (default: 5,10).",
    )
    p.add_argument(
        "--golden-features",
        action="store_true",
        help="Enable AutoML golden features generation in mljar (disabled by default).",
    )
    p.add_argument(
        "--features-selection",
        action="store_true",
        help="Enable AutoML feature selection in mljar (disabled by default).",
    )
    p.add_argument(
        "--stack-models",
        action="store_true",
        help="Enable model stacking in mljar (disabled by default).",
    )
    p.add_argument(
        "--train-ensemble",
        action="store_true",
        help="Enable training of ensembles in mljar (disabled by default).",
    )
    p.add_argument(
        "--target-transform",
        choices=["auto", "none", "pct", "log"],
        default="auto",
        help="Transform target for modeling: 'pct' for percentage return, 'log' for log return, 'none' for raw. "
             "'auto' uses 'log' when --predict-next, else 'none' (default: auto).",
    )
    p.add_argument(
        "--clip-prediction-pct",
        type=float,
        default=0.0,
        help="If > 0, clip next-step predicted price within +/- this percent of the last observed price (default: 0 - disabled).",
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
    # Determine time column: prefer --time-column; fallback to common names, then normalize to canonical 't'
    preferred_time_col = args.time_column
    detected_time_col = None
    if preferred_time_col and preferred_time_col in df.columns:
        detected_time_col = preferred_time_col
    else:
        for cand in ["t", "Timestamp", "timestamp", "time", "date", "datetime", "Datetime", "ts"]:
            if cand in df.columns:
                detected_time_col = cand
                break
    # Normalize time column to 't', coerce to tz-aware datetime, and sort ascending (past -> present)
    expectedtime_iso = None
    X_next = None
    original_time_col = None
    if detected_time_col:
        original_time_col = detected_time_col
        df["t"] = pd.to_datetime(df[detected_time_col], utc=True, errors="coerce")
        df = df.sort_values("t").reset_index(drop=True)
        if detected_time_col != "t":
            print(f"[mljar] Using time column: t (normalized from {detected_time_col})")
        else:
            print(f"[mljar] Using time column: t")
    else:
        print("[mljar] Warning: No time column found. Provide one via --time-column (e.g., 't').")
    # Basic time-aware feature engineering
    try:
        lags = _parse_int_list(getattr(args, "lags", None) or "1,2,3")
        windows = _parse_int_list(getattr(args, "rolling_windows", None) or "5,10")
        if "t" in df.columns:
            # Drop duplicate timestamps to avoid leakage
            df = df.drop_duplicates(subset=["t"], keep="last")
        df = _featurize(df, args.target, "t" if "t" in df.columns else None, lags, windows)
    except Exception as e:
        print(f"[mljar] Featurization skipped due to error: {e}")
    # If next-step forecasting is requested, shift target and prepare next-step features
    if getattr(args, "predict_next", False):
        if not detected_time_col:
            raise SystemExit("--predict-next requires a time column in the data (e.g., 't' or 'Timestamp').")
        target_name = "TargetNext"
        base_price_full = pd.to_numeric(df[args.target], errors="coerce")
        df[target_name] = base_price_full.shift(-1)
        if len(df) < 2:
            raise SystemExit("Not enough rows to compute next-step prediction")
        # Compute expected time for next bar: last observed time + median interval (fallback 1 day)
        time_diffs = df["t"].diff()
        try:
            step = time_diffs.median()
        except Exception:
            step = None
        if not isinstance(step, pd.Timedelta) or pd.isna(step) or step == pd.Timedelta(0):
            step = pd.Timedelta(days=1)
        last_time = df["t"].iloc[-1]
        expectedtime_iso = (last_time + step).strftime("%Y-%m-%dT%H:%M:%SZ")
        # Build X_next from the last available feature row (index -1)
        drop_cols_for_next = [args.target, target_name, "t"] + (
            [original_time_col] if original_time_col and original_time_col != "t" else []
        )
        feature_cols = [c for c in df.columns if c not in drop_cols_for_next]
        X_next = df.iloc[[-1]][feature_cols]
        last_base_price = float(base_price_full.iloc[-1]) if pd.notna(base_price_full.iloc[-1]) else None
        # Drop last row (NaN shifted target) and replace target with shifted target to avoid duplicate column names
        df = df.iloc[:-1].copy()
        df[args.target] = df[target_name]
        # Keep aligned base and next-price series for transforms/metrics
        base_series = base_price_full.iloc[:-1]
        y_price_series = df[target_name].copy()
        df = df.drop(columns=[target_name])
    # Keep time series for potential splitting (non next-step path)
    times_series = df["t"] if "t" in df.columns else None
    # Split X and y
    X, y = _split_xy(df, args.target)
    # Ensure target is a 1-D Series
    if isinstance(y, pd.DataFrame):
        if y.shape[1] == 1:
            y = y.iloc[:, 0]
        else:
            raise SystemExit(f"Target '{args.target}' produced multiple columns; please specify a single target column.")
    # Determine target transform
    transform_kind = getattr(args, "target_transform", "auto")
    if transform_kind == "auto":
        transform_kind = "log" if getattr(args, "predict_next", False) else "none"
    # If predicting next-step, optionally transform the target into returns
    if getattr(args, "predict_next", False) and transform_kind in ("pct", "log"):
        if "base_series" not in locals():
            raise SystemExit("Internal error: base_series not computed for target transform")
        y = _apply_target_transform(transform_kind, y, base_series)
    # Drop rows where target is missing
    if y.isna().values.any():
        mask = ~y.isna().values
        X = X.loc[mask].reset_index(drop=True)
        y = y.loc[mask].reset_index(drop=True)
        if getattr(args, "predict_next", False) and "base_series" in locals():
            base_series = base_series[mask].reset_index(drop=True)
            y_price_series = y_price_series[mask].reset_index(drop=True)
    # Drop time columns from features to prevent leakage
    drop_time_cols = []
    if "t" in X.columns:
        drop_time_cols.append("t")
    if original_time_col and original_time_col in X.columns:
        drop_time_cols.append(original_time_col)
    if drop_time_cols:
        X = X.drop(columns=drop_time_cols)
    task_hint = _detect_task(y)

    # Stratify only for classification
    stratify = None
    if not args.no_stratify and "classification" in task_hint:
        try:
            stratify = y
        except Exception:
            stratify = None

    if getattr(args, "predict_next", False):
        X_train, X_test, y_train, y_test, base_train, base_test, yprice_train, yprice_test = train_test_split(
            X, y, base_series, y_price_series, test_size=args.test_size, random_state=args.random_state, shuffle=False
        )
    elif times_series is not None:
        # Preserve chronological order for time-series data; stratification is incompatible with shuffle=False
        X_train, X_test, y_train, y_test, t_train, t_test = train_test_split(
            X, y, times_series, test_size=args.test_size, random_state=args.random_state, shuffle=False, stratify=None
        )
    else:
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=args.test_size, random_state=args.random_state, stratify=stratify
        )

    algorithms = _comma_list(args.algorithms)
    # Provide a stronger default algorithm set when tuning is requested and none specified
    if getattr(args, "tune_strong", False) and args.algorithms is None:
        algorithms = ["LightGBM", "Xgboost", "CatBoost", "Random Forest", "Extra Trees", "Linear"]

    # Select a default evaluation metric if none provided.
    # For ClosePrice regression we default to RMSE; for classification we use logloss.
    eval_metric = args.eval_metric or ("rmse" if "regression" in task_hint else "logloss")

    # Build validation strategy
    validation_strategy = None
    tr = max(0.5, min(float(args.train_ratio), 0.95))
    # mljar-supervised doesn't support 'time' validation in the current version; use split with shuffle control
    if args.validation_type == "auto":
        if args.predict_next or ("t" in df.columns):
            print("[mljar] Using split validation (fallback from time) for time-ordered data")
            validation_strategy = {"validation_type": "split", "train_ratio": tr, "shuffle": False}
        else:
            validation_strategy = {"validation_type": "split", "train_ratio": tr, "shuffle": True}
    elif args.validation_type == "time":
        print("[mljar] validation_type 'time' not supported; falling back to 'split'")
        validation_strategy = {"validation_type": "split", "train_ratio": tr, "shuffle": False}
    elif args.validation_type == "split":
        validation_strategy = {
            "validation_type": "split",
            "train_ratio": tr,
            "shuffle": not (args.predict_next or ("t" in df.columns)),
        }
    elif args.validation_type == "kfold":
        kf = max(2, int(args.k_folds))
        validation_strategy = {"validation_type": "kfold", "k_folds": kf}

    # Potentially strengthen mode when requested
    mode_use = args.mode
    if getattr(args, "tune_strong", False) and args.mode == "Explain":
        mode_use = "Compete"
        print("[mljar] tune-strong enabled: switching mode to 'Compete' for stronger tuning")

    automl_kwargs = dict(
        mode=mode_use,
        total_time_limit=args.time_limit,
        results_path=results_path,
        eval_metric=eval_metric,
        explain_level=args.explain_level,
        random_state=args.random_state,
        verbose=args.verbose,
        ml_task=task_hint,
    )
    if validation_strategy:
        automl_kwargs["validation_strategy"] = validation_strategy
    if algorithms:
        automl_kwargs["algorithms"] = algorithms
    # Optional stronger modeling features (opt-in)
    if getattr(args, "golden_features", False):
        automl_kwargs["golden_features"] = True
    if getattr(args, "features_selection", False):
        automl_kwargs["features_selection"] = True
    if getattr(args, "stack_models", False):
        automl_kwargs["stack_models"] = True
    if getattr(args, "train_ensemble", False):
        automl_kwargs["train_ensemble"] = True

    automl = AutoML(**automl_kwargs)

    print(f"[mljar] Starting AutoML in mode={automl_kwargs.get('mode')} time_limit={args.time_limit}s")
    print(f"[mljar] Results path: {results_path}")
    print(f"[mljar] Detected task hint: {task_hint}")

    automl.fit(X_train, y_train)

    # Evaluate on holdout
    preds = automl.predict(X_test)
    # Compute next-step prediction if requested
    next_pred = None
    if getattr(args, "predict_next", False) and 'X_next' in locals() and X_next is not None:
        try:
            raw_pred = float(automl.predict(X_next)[0])
            # Invert transform to price if needed
            if transform_kind in ("pct", "log"):
                if 'last_base_price' in locals() and last_base_price is not None:
                    next_pred = float(_invert_target_transform(transform_kind, pd.Series([last_base_price]), pd.Series([raw_pred])).iloc[0])
                else:
                    next_pred = None
                    print("[mljar] Warning: Missing last_base_price; cannot invert transformed prediction to price")
            else:
                next_pred = raw_pred
            # Optional clipping
            clip_pct = float(getattr(args, "clip_prediction_pct", 0.0) or 0.0)
            if next_pred is not None and clip_pct > 0 and 'last_base_price' in locals() and last_base_price is not None:
                lower = last_base_price * (1.0 - clip_pct / 100.0)
                upper = last_base_price * (1.0 + clip_pct / 100.0)
                clipped = min(max(next_pred, lower), upper)
                if clipped != next_pred:
                    print(f"[mljar] Clipped next-step prediction from {next_pred:.6f} to {clipped:.6f} using ±{clip_pct:.2f}% band around {last_base_price:.6f}")
                next_pred = clipped
            if next_pred is not None:
                print(f"[mljar] Next-step prediction: {args.target}={next_pred}")
        except Exception as e:
            print(f"[mljar] Failed to compute next-step prediction: {e}")
    # Compute holdout RMSE for regression tasks
    rmse = None
    try:
        if "classification" in task_hint:
            rmse = None
            print("[mljar] Skipping RMSE: classification task detected")
        else:
            from sklearn.metrics import mean_squared_error
            if getattr(args, "predict_next", False) and transform_kind in ("pct", "log"):
                # Evaluate in price space
                preds_price = _invert_target_transform(transform_kind, base_test, preds)
                y_true_price = pd.Series(yprice_test).astype(float).reset_index(drop=True)
                preds_price = pd.Series(preds_price).astype(float).reset_index(drop=True)
                m = min(len(y_true_price), len(preds_price))
                if m > 1:
                    rmse = float(np.sqrt(mean_squared_error(y_true_price.iloc[:m], preds_price.iloc[:m])))
                    print(f"[mljar] Holdout RMSE (price): {rmse:.6f}")
                else:
                    print("[mljar] Not enough samples to compute RMSE in price space")
            else:
                y_true_np = np.asarray(y_test, dtype=float).reshape(-1)
                preds_np = np.asarray(preds, dtype=float).reshape(-1)
                m = min(len(y_true_np), len(preds_np))
                if m > 1:
                    rmse = float(np.sqrt(mean_squared_error(y_true_np[:m], preds_np[:m])))
                    print(f"[mljar] Holdout RMSE: {rmse:.6f}")
                else:
                    print("[mljar] Not enough samples to compute RMSE")
    except Exception as e:
        print(f"[mljar] Skipping RMSE computation due to: {e}")

    # Save predictions summary to MongoDB 'prediction' collection
    try:
        sym = str(args.symbol).strip().upper() if args.symbol else None
        source_collection = args.mongo_collection or args.collection or "csv"
        out_collection = args.output_collection or "prediction"
        client = _connect_mongo()
        db_name = args.mongo_db or os.getenv("MONGODB_DB") or "TradingApp"
        out_col = client[db_name][out_collection]

        if getattr(args, "predict_next", False):
            if not sym or not expectedtime_iso or next_pred is None:
                print("[mljar] Skipping MongoDB save: missing symbol, expected time, or next prediction")
            else:
                out_col.update_one(
                    {"symbol": sym, "collection": source_collection, "expectedtime": expectedtime_iso},
                    {"$set": {
                        "symbol": sym,
                        "collection": source_collection,
                        "expectedtime": expectedtime_iso,
                        "rmse": rmse,
                        args.target: float(next_pred),
                    }},
                    upsert=True,
                )
                print(f"[mljar] Saved next-step prediction to MongoDB collection '{out_collection}' at expectedtime={expectedtime_iso}")
        else:
            if sym and 't_test' in locals():
                times_iso = (
                    pd.to_datetime(pd.Series(t_test), utc=True, errors='coerce')
                    .dt.strftime('%Y-%m-%dT%H:%M:%SZ')
                    .tolist()
                )
                preds_list = preds.tolist() if hasattr(preds, "tolist") else list(preds)
                pairs = []
                for iso, pred in zip(times_iso, preds_list):
                    if not isinstance(iso, str) or not iso:
                        continue
                    if isinstance(pred, (list, tuple)):
                        pred_val = pred[0] if len(pred) > 0 else None
                    else:
                        try:
                            pred_val = float(pred)
                        except Exception:
                            pred_val = None
                    if pred_val is None:
                        continue
                    pairs.append((iso, pred_val))
                if pairs:
                    latest_iso = max(iso for iso, _ in pairs)
                    latest_pred = next(val for iso, val in pairs if iso == latest_iso)
                    out_col.update_one(
                        {"symbol": sym, "collection": source_collection, "expectedtime": latest_iso},
                        {"$set": {
                            "symbol": sym,
                            "collection": source_collection,
                            "expectedtime": latest_iso,
                            "rmse": rmse,
                            args.target: latest_pred,
                        }},
                        upsert=True,
                    )
                    print(f"[mljar] Saved latest prediction record to MongoDB collection '{out_collection}' at expectedtime={latest_iso}")
                else:
                    print("[mljar] No valid (time, prediction) pairs to save")
            else:
                print("[mljar] Skipping MongoDB save: missing symbol or time column not found in data")
    except Exception as e:
        print(f"[mljar] Failed to save predictions to MongoDB: {e}")

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
