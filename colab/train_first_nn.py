"""
Google Colab script: first neural network using vw_muestreos_parametros_wide.

Quick start in Colab:
1) Install deps:
   !pip -q install pandas numpy scikit-learn tensorflow joblib matplotlib seaborn
2) Upload CSV to Colab as /content/nn.csv
3) Run:
   %run colab/train_first_nn.py

Required CSV:
- /content/nn.csv
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import joblib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.metrics import (
    accuracy_score,
    classification_report,
    confusion_matrix,
    f1_score,
    precision_recall_curve,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.utils.class_weight import compute_class_weight
from tensorflow import keras
from tensorflow.keras import layers


SEED = 42
np.random.seed(SEED)
keras.utils.set_random_seed(SEED)

CSV_PATH = Path("/content/drive/MyDrive/Colab Docs/nn.csv")
OUTPUT_DIR = CSV_PATH.parent / "modelo_sat_nn"
FEATURE_COLUMNS = [
    "coliformes_fecales",
    "coliformes_totales",
    "nitratos",
    "nitritos",
    "amonio",
    "nitrogeno_total",
    "ph",
    "conductividad",
    "temperatura",
    "turbidez",
    "cloro_residual",
    "solidos_totales",
    "materia_organica",
    "alcalinidad_total",
    "alcalinidad_fenolftaleina",
    "bicarbonato",
    "carbonato",
    "sulfato",
    "cloruro",
    "dureza",
    "calcio",
    "magnesio",
    "sodio",
    "potasio",
    "hierro_total",
    "fluoruro",
    "arsenico",
    "mercurio",
    "manganeso",
    "cobre",
    "cromo_total",
]


@dataclass
class DatasetBundle:
    x: pd.DataFrame
    y: pd.Series
    numeric_cols: list[str]
    categorical_cols: list[str]
    dropped_cols: list[str]


def _make_one_hot_encoder() -> OneHotEncoder:
    try:
        return OneHotEncoder(handle_unknown="ignore", sparse_output=False)
    except TypeError:
        return OneHotEncoder(handle_unknown="ignore", sparse=False)


def load_dataframe() -> pd.DataFrame:
    if not CSV_PATH.exists():
        raise FileNotFoundError(f"CSV not found: {CSV_PATH}")

    df = pd.read_csv(CSV_PATH)
    print(f"Loaded {len(df)} rows from csv: {CSV_PATH}")
    return df


def normalize_target(target: pd.Series) -> pd.Series:
    if pd.api.types.is_bool_dtype(target):
        return target.astype(int)

    normalized = (
        target.astype(str)
        .str.strip()
        .str.lower()
        .replace({"nan": np.nan, "none": np.nan, "": np.nan})
    )

    mapping = {
        "true": 1,
        "false": 0,
        "t": 1,
        "f": 0,
        "1": 1,
        "0": 0,
        "si": 1,
        "no": 0,
    }

    return normalized.map(mapping)


def prepare_dataset(df: pd.DataFrame) -> DatasetBundle:
    if "contaminacion_observada" not in df.columns:
        raise KeyError("Missing required column: contaminacion_observada")

    y_raw = normalize_target(df["contaminacion_observada"])
    valid_mask = y_raw.notna()
    df = df.loc[valid_mask].copy()
    y = y_raw.loc[valid_mask].astype(int)

    selected_feature_cols = [c for c in FEATURE_COLUMNS if c in df.columns]
    missing_feature_cols = [c for c in FEATURE_COLUMNS if c not in df.columns]
    if not selected_feature_cols:
        raise ValueError(
            "No parameter columns found for training. "
            "Expected at least one known feature from vw_muestreos_parametros_wide."
        )

    x = df[selected_feature_cols].copy()

    bool_cols = x.select_dtypes(include=["bool"]).columns.tolist()
    for col in bool_cols:
        x[col] = x[col].astype(int)

    for col in x.columns:
        x[col] = pd.to_numeric(x[col], errors="coerce")

    all_null_cols = [c for c in x.columns if x[c].isna().all()]
    if all_null_cols:
        x = x.drop(columns=all_null_cols)

    numeric_cols = x.select_dtypes(include=[np.number]).columns.tolist()
    categorical_cols = [c for c in x.columns if c not in numeric_cols]

    max_unique = 50
    high_card_cols = [
        c for c in categorical_cols if x[c].nunique(dropna=True) > max_unique
    ]
    if high_card_cols:
        x = x.drop(columns=high_card_cols)
        categorical_cols = [c for c in categorical_cols if c not in high_card_cols]

    print("Rows for training:", len(x))
    print("Class distribution:")
    print(y.value_counts(dropna=False).sort_index())
    print("Numeric features:", len(numeric_cols))
    print("Categorical features:", len(categorical_cols))
    print("Selected feature columns:", selected_feature_cols)
    if missing_feature_cols:
        print("Feature columns missing in CSV:", missing_feature_cols)
    if all_null_cols:
        print("Dropped all-null columns:", all_null_cols)
    if high_card_cols:
        print("Dropped high-cardinality columns:", high_card_cols)

    return DatasetBundle(
        x=x,
        y=y,
        numeric_cols=numeric_cols,
        categorical_cols=categorical_cols,
        dropped_cols=missing_feature_cols + all_null_cols + high_card_cols,
    )


def build_preprocessor(numeric_cols: list[str], categorical_cols: list[str]) -> ColumnTransformer:
    numeric_pipeline = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="median")),
            ("scaler", StandardScaler()),
        ]
    )

    categorical_pipeline = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="most_frequent")),
            ("onehot", _make_one_hot_encoder()),
        ]
    )

    transformers = []
    if numeric_cols:
        transformers.append(("num", numeric_pipeline, numeric_cols))
    if categorical_cols:
        transformers.append(("cat", categorical_pipeline, categorical_cols))

    if not transformers:
        raise ValueError("No usable features after preprocessing.")

    return ColumnTransformer(transformers=transformers, remainder="drop", sparse_threshold=0.0)


def build_model(input_dim: int) -> keras.Model:
    model = keras.Sequential(
        [
            layers.Input(shape=(input_dim,)),
            layers.BatchNormalization(),
            layers.Dense(128, activation="relu"),
            layers.Dropout(0.30),
            layers.Dense(64, activation="relu"),
            layers.Dropout(0.25),
            layers.Dense(32, activation="relu"),
            layers.Dropout(0.20),
            layers.Dense(16, activation="relu"),
            layers.Dropout(0.15),
            layers.Dense(1, activation="sigmoid"),
        ]
    )

    model.compile(
        optimizer=keras.optimizers.Adam(learning_rate=1e-3),
        loss="binary_crossentropy",
        metrics=[
            keras.metrics.BinaryAccuracy(name="accuracy"),
            keras.metrics.AUC(name="auc"),
            keras.metrics.Precision(name="precision"),
            keras.metrics.Recall(name="recall"),
        ],
    )
    return model


def best_f1_threshold(y_true: np.ndarray, y_prob: np.ndarray) -> float:
    precision, recall, thresholds = precision_recall_curve(y_true, y_prob)
    f1_scores = (2 * precision * recall) / (precision + recall + 1e-12)
    best_idx = int(np.nanargmax(f1_scores))

    if best_idx == 0 or len(thresholds) == 0:
        return 0.5
    return float(thresholds[best_idx - 1])


def main() -> None:
    df = load_dataframe()
    data = prepare_dataset(df)

    x_train_val, x_test, y_train_val, y_test = train_test_split(
        data.x,
        data.y,
        test_size=0.20,
        stratify=data.y,
        random_state=SEED,
    )

    x_train, x_val, y_train, y_val = train_test_split(
        x_train_val,
        y_train_val,
        test_size=0.25,
        stratify=y_train_val,
        random_state=SEED,
    )

    preprocessor = build_preprocessor(data.numeric_cols, data.categorical_cols)

    x_train_proc = preprocessor.fit_transform(x_train).astype(np.float32)
    x_val_proc = preprocessor.transform(x_val).astype(np.float32)
    x_test_proc = preprocessor.transform(x_test).astype(np.float32)

    class_weights_values = compute_class_weight(
        class_weight="balanced",
        classes=np.array([0, 1]),
        y=y_train.to_numpy(),
    )
    class_weight = {0: float(class_weights_values[0]), 1: float(class_weights_values[1])}
    print("Class weight:", class_weight)

    model = build_model(input_dim=x_train_proc.shape[1])

    callbacks = [
        keras.callbacks.EarlyStopping(
            monitor="val_auc",
            mode="max",
            patience=20,
            restore_best_weights=True,
        ),
        keras.callbacks.ReduceLROnPlateau(
            monitor="val_auc",
            mode="max",
            factor=0.5,
            patience=8,
            min_lr=1e-5,
        ),
    ]

    history = model.fit(
        x_train_proc,
        y_train.to_numpy(),
        validation_data=(x_val_proc, y_val.to_numpy()),
        epochs=250,
        batch_size=16,
        class_weight=class_weight,
        callbacks=callbacks,
        verbose=1,
    )

    y_val_prob = model.predict(x_val_proc, verbose=0).ravel()
    threshold = best_f1_threshold(y_val.to_numpy(), y_val_prob)
    print(f"Best threshold on validation (F1): {threshold:.4f}")

    y_test_prob = model.predict(x_test_proc, verbose=0).ravel()
    y_test_pred = (y_test_prob >= threshold).astype(int)

    metrics = {
        "accuracy": float(accuracy_score(y_test, y_test_pred)),
        "precision": float(precision_score(y_test, y_test_pred, zero_division=0)),
        "recall": float(recall_score(y_test, y_test_pred, zero_division=0)),
        "f1": float(f1_score(y_test, y_test_pred, zero_division=0)),
        "roc_auc": float(roc_auc_score(y_test, y_test_prob)),
        "threshold": float(threshold),
    }

    print("\nTest metrics:")
    for k, v in metrics.items():
        print(f"- {k}: {v:.4f}")

    print("\nClassification report:")
    print(classification_report(y_test, y_test_pred, digits=4, zero_division=0))

    cm = confusion_matrix(y_test, y_test_pred)
    print("Confusion matrix:")
    print(cm)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    model_path = OUTPUT_DIR / "red_neuronal_contaminacion.keras"
    preprocessor_path = OUTPUT_DIR / "preprocesador.joblib"
    metadata_path = OUTPUT_DIR / "resumen_entrenamiento.json"

    model.save(model_path)
    joblib.dump(preprocessor, preprocessor_path)

    try:
        feature_names = preprocessor.get_feature_names_out().tolist()
    except Exception:
        feature_names = []

    metadata = {
        "seed": SEED,
        "n_rows": int(len(data.x)),
        "n_features_input": int(data.x.shape[1]),
        "n_features_model": int(x_train_proc.shape[1]),
        "numeric_cols": data.numeric_cols,
        "categorical_cols": data.categorical_cols,
        "dropped_cols": data.dropped_cols,
        "class_weight": class_weight,
        "metrics_test": metrics,
        "feature_names_after_preprocess": feature_names,
    }

    with metadata_path.open("w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2, ensure_ascii=False)

    print("\nArtifacts saved:")
    print("-", model_path)
    print("-", preprocessor_path)
    print("-", metadata_path)

    plt.figure(figsize=(10, 4))
    plt.subplot(1, 2, 1)
    plt.plot(history.history.get("loss", []), label="train")
    plt.plot(history.history.get("val_loss", []), label="val")
    plt.title("Loss")
    plt.legend()

    plt.subplot(1, 2, 2)
    plt.plot(history.history.get("auc", []), label="train")
    plt.plot(history.history.get("val_auc", []), label="val")
    plt.title("AUC")
    plt.legend()
    plt.tight_layout()
    plt.show()


if __name__ == "__main__":
    main()
