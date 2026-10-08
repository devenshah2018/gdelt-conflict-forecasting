"""Metric computations for rare-event classification.

Accuracy is meaningless when spikes are rare, so everything here centers on
the positive class: PR-AUC, precision/recall/F1 at an operating threshold
chosen on the training set, recall at a fixed false alarm rate, and a
calibration curve. Implemented with numpy on collected prediction arrays
(the test sets are small once aggregated to country-weeks).
"""
from __future__ import annotations

import numpy as np

from . import config


def pr_auc(y_true: np.ndarray, y_prob: np.ndarray) -> float:
    """Average precision (step-wise area under the precision-recall curve)."""
    order = np.argsort(-y_prob, kind="stable")
    y = y_true[order]
    tp = np.cumsum(y)
    total_pos = y.sum()
    if total_pos == 0:
        return float("nan")
    precision = tp / np.arange(1, len(y) + 1)
    recall = tp / total_pos
    recall_prev = np.concatenate([[0.0], recall[:-1]])
    return float(np.sum((recall - recall_prev) * precision))


def roc_auc(y_true: np.ndarray, y_prob: np.ndarray) -> float:
    """Rank-based ROC AUC (Mann-Whitney U statistic)."""
    pos = y_prob[y_true == 1]
    neg = y_prob[y_true == 0]
    if len(pos) == 0 or len(neg) == 0:
        return float("nan")
    # Tie-aware average ranks.
    all_scores = np.concatenate([pos, neg])
    sorter = np.argsort(all_scores, kind="stable")
    inv = np.empty_like(sorter)
    inv[sorter] = np.arange(len(all_scores))
    sorted_scores = all_scores[sorter]
    # Average rank per tied group.
    _, first_idx, counts = np.unique(sorted_scores, return_index=True, return_counts=True)
    avg_ranks_sorted = np.repeat(first_idx + (counts + 1) / 2.0, counts)
    avg_ranks = avg_ranks_sorted[inv]
    rank_sum_pos = avg_ranks[: len(pos)].sum()
    u = rank_sum_pos - len(pos) * (len(pos) + 1) / 2.0
    return float(u / (len(pos) * len(neg)))


def precision_recall_f1(y_true: np.ndarray, y_pred: np.ndarray) -> dict:
    """Precision, recall, and F1 for binary predictions on the positive class."""
    tp = int(np.sum((y_pred == 1) & (y_true == 1)))
    fp = int(np.sum((y_pred == 1) & (y_true == 0)))
    fn = int(np.sum((y_pred == 0) & (y_true == 1)))
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    return {
        "precision": round(precision, 4),
        "recall": round(recall, 4),
        "f1": round(f1, 4),
        "tp": tp,
        "fp": fp,
        "fn": fn,
    }


def best_f1_threshold(y_true: np.ndarray, y_prob: np.ndarray) -> float:
    """Threshold maximizing F1. Choose this on TRAIN predictions only."""
    candidates = np.unique(np.round(y_prob, 3))
    if len(candidates) == 0:
        return 0.5
    best_t, best_f1 = 0.5, -1.0
    for t in candidates:
        m = precision_recall_f1(y_true, (y_prob >= t).astype(int))
        if m["f1"] > best_f1:
            best_f1, best_t = m["f1"], float(t)
    return best_t


def recall_at_far(y_true: np.ndarray, y_prob: np.ndarray, far: float | None = None) -> dict:
    """Recall when the threshold is set so the false alarm rate is `far`.

    The threshold is the (1 - far) quantile of the negative-class scores, so
    only `far` of the quiet country-weeks would trigger an alert.
    """
    far = far if far is not None else config.TARGET_FAR
    neg = y_prob[y_true == 0]
    pos = y_prob[y_true == 1]
    if len(neg) == 0 or len(pos) == 0:
        return {"recall_at_far": float("nan"), "threshold": float("nan"), "realized_far": float("nan")}
    threshold = float(np.quantile(neg, 1.0 - far))
    recall = float(np.mean(pos >= threshold))
    realized = float(np.mean(neg >= threshold))
    return {
        "recall_at_far": round(recall, 4),
        "threshold": round(threshold, 4),
        "realized_far": round(realized, 4),
    }


def calibration_curve(y_true: np.ndarray, y_prob: np.ndarray, bins: int | None = None) -> list[dict]:
    """Quantile-binned calibration: mean predicted vs observed spike rate."""
    bins = bins or config.CALIBRATION_BINS
    if len(y_prob) == 0:
        return []
    edges = np.quantile(y_prob, np.linspace(0, 1, bins + 1))
    edges = np.unique(edges)
    out = []
    for lo, hi in zip(edges[:-1], edges[1:]):
        mask = (y_prob >= lo) & (y_prob <= hi if hi == edges[-1] else y_prob < hi)
        if mask.sum() == 0:
            continue
        out.append(
            {
                "mean_predicted": round(float(y_prob[mask].mean()), 4),
                "observed_rate": round(float(y_true[mask].mean()), 4),
                "count": int(mask.sum()),
            }
        )
    return out


def evaluate_probabilistic(
    y_train: np.ndarray,
    p_train: np.ndarray,
    y_test: np.ndarray,
    p_test: np.ndarray,
) -> dict:
    """Full metric suite for a probabilistic model.

    The F1 operating threshold is chosen on train predictions and then
    applied to the test set, so the test set never influences its own
    operating point.
    """
    threshold = best_f1_threshold(y_train, p_train)
    at_threshold = precision_recall_f1(y_test, (p_test >= threshold).astype(int))
    metrics = {
        "pr_auc": round(pr_auc(y_test, p_test), 4),
        "roc_auc": round(roc_auc(y_test, p_test), 4),
        "threshold": round(threshold, 4),
        **at_threshold,
        **recall_at_far(y_test, p_test),
        "calibration": calibration_curve(y_test, p_test),
    }
    return metrics
