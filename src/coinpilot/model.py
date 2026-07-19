from __future__ import annotations

from dataclasses import dataclass

import numpy as np


@dataclass(frozen=True, slots=True)
class ModelFitSummary:
    samples: int
    features: int
    positive_rate: float
    iterations: int
    loss: float


@dataclass(frozen=True, slots=True)
class RidgeFitSummary:
    samples: int
    features: int
    target_mean: float
    target_std: float
    rmse: float
    clip_lower: float
    clip_upper: float

    @property
    def clip_bounds(self) -> tuple[float, float]:
        return self.clip_lower, self.clip_upper


class StandardizedLogisticRegression:
    """Deterministic L2 logistic regression implemented with NumPy."""

    def __init__(
        self,
        *,
        l2: float = 0.01,
        learning_rate: float = 0.08,
        max_iterations: int = 250,
        tolerance: float = 1e-8,
    ) -> None:
        self.l2 = l2
        self.learning_rate = learning_rate
        self.max_iterations = max_iterations
        self.tolerance = tolerance
        self.mean_: np.ndarray | None = None
        self.scale_: np.ndarray | None = None
        self.coef_: np.ndarray | None = None
        self.intercept_: float | None = None
        self.summary_: ModelFitSummary | None = None

    @staticmethod
    def _sigmoid(values: np.ndarray) -> np.ndarray:
        clipped = np.clip(values, -35.0, 35.0)
        return 1.0 / (1.0 + np.exp(-clipped))

    def fit(self, features: np.ndarray, target: np.ndarray) -> ModelFitSummary:
        x = np.asarray(features, dtype=float)
        y = np.asarray(target, dtype=float).reshape(-1)
        if x.ndim != 2:
            raise ValueError("features must be a two-dimensional array")
        if len(x) != len(y) or len(y) == 0:
            raise ValueError("features and target must have matching non-zero rows")
        if not np.isfinite(x).all() or not np.isfinite(y).all():
            raise ValueError("training data must be finite")
        if not np.isin(y, (0.0, 1.0)).all():
            raise ValueError("target must contain only zero and one")

        self.mean_ = x.mean(axis=0)
        scale = x.std(axis=0)
        self.scale_ = np.where(scale < 1e-12, 1.0, scale)
        normalized = (x - self.mean_) / self.scale_

        samples, feature_count = normalized.shape
        positive_rate = float(y.mean())
        coefficients = np.zeros(feature_count, dtype=float)
        clipped_rate = float(np.clip(positive_rate, 1e-6, 1 - 1e-6))
        intercept = float(np.log(clipped_rate / (1 - clipped_rate)))

        positives = max(float(y.sum()), 1.0)
        negatives = max(float(samples - y.sum()), 1.0)
        sample_weights = np.where(
            y == 1.0,
            samples / (2.0 * positives),
            samples / (2.0 * negatives),
        )

        previous_loss = np.inf
        final_loss = np.inf
        iteration = 0
        for iteration in range(1, self.max_iterations + 1):
            probabilities = self._sigmoid(normalized @ coefficients + intercept)
            error = (probabilities - y) * sample_weights
            coefficient_gradient = (
                normalized.T @ error / samples + self.l2 * coefficients
            )
            intercept_gradient = float(error.mean())
            coefficients -= self.learning_rate * coefficient_gradient
            intercept -= self.learning_rate * intercept_gradient

            probabilities = self._sigmoid(normalized @ coefficients + intercept)
            cross_entropy = -np.mean(
                sample_weights
                * (
                    y * np.log(np.clip(probabilities, 1e-12, 1.0))
                    + (1 - y)
                    * np.log(np.clip(1 - probabilities, 1e-12, 1.0))
                )
            )
            final_loss = float(
                cross_entropy + 0.5 * self.l2 * np.dot(coefficients, coefficients)
            )
            if abs(previous_loss - final_loss) < self.tolerance:
                break
            previous_loss = final_loss

        self.coef_ = coefficients
        self.intercept_ = intercept
        self.summary_ = ModelFitSummary(
            samples=samples,
            features=feature_count,
            positive_rate=positive_rate,
            iterations=iteration,
            loss=final_loss,
        )
        return self.summary_

    def predict_proba(self, features: np.ndarray) -> np.ndarray:
        if (
            self.mean_ is None
            or self.scale_ is None
            or self.coef_ is None
            or self.intercept_ is None
        ):
            raise RuntimeError("Model has not been fitted")
        x = np.asarray(features, dtype=float)
        if x.ndim == 1:
            x = x.reshape(1, -1)
        if x.ndim != 2 or x.shape[1] != len(self.coef_):
            raise ValueError("Prediction feature shape does not match the model")
        if not np.isfinite(x).all():
            raise ValueError("Prediction features must be finite")
        normalized = (x - self.mean_) / self.scale_
        return self._sigmoid(normalized @ self.coef_ + self.intercept_)


class StandardizedRidgeRegression:
    """Deterministic ridge regression for continuous, winsorized targets.

    Feature standardization and symmetric target clip bounds are learned only
    from the data passed to :meth:`fit`. The ridge objective uses mean squared
    error, so its closed-form system adds ``l2`` to the feature covariance
    diagonal. Predictions are deliberately not clipped to the training target
    bounds.
    """

    def __init__(
        self,
        *,
        l2: float = 0.01,
        clip_quantile: float = 0.01,
    ) -> None:
        try:
            l2_value = float(l2)
        except (TypeError, ValueError) as exc:
            raise ValueError("l2 must be a finite non-negative value") from exc
        try:
            clip_quantile_value = float(clip_quantile)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                "clip_quantile must be finite and in [0, 0.5)"
            ) from exc

        if not np.isfinite(l2_value) or l2_value < 0:
            raise ValueError("l2 must be a finite non-negative value")
        if (
            not np.isfinite(clip_quantile_value)
            or clip_quantile_value < 0
            or clip_quantile_value >= 0.5
        ):
            raise ValueError("clip_quantile must be finite and in [0, 0.5)")

        self.l2 = l2_value
        self.clip_quantile = clip_quantile_value
        self.mean_: np.ndarray | None = None
        self.scale_: np.ndarray | None = None
        self.coef_: np.ndarray | None = None
        self.intercept_: float | None = None
        self.target_mean_: float | None = None
        self.target_std_: float | None = None
        self.clip_lower_: float | None = None
        self.clip_upper_: float | None = None
        self.summary_: RidgeFitSummary | None = None

    def fit(self, features: np.ndarray, target: np.ndarray) -> RidgeFitSummary:
        x = np.asarray(features, dtype=float)
        y = np.asarray(target, dtype=float).reshape(-1)
        if x.ndim != 2:
            raise ValueError("features must be a two-dimensional array")
        if x.shape[1] == 0:
            raise ValueError("features must contain at least one column")
        if len(x) != len(y) or len(y) == 0:
            raise ValueError("features and target must have matching non-zero rows")
        if not np.isfinite(x).all() or not np.isfinite(y).all():
            raise ValueError("training data must be finite")

        feature_mean = x.mean(axis=0)
        feature_std = x.std(axis=0)
        feature_scale = np.where(feature_std < 1e-12, 1.0, feature_std)
        normalized = (x - feature_mean) / feature_scale
        if not np.isfinite(normalized).all():
            raise ValueError("features are too large to standardize safely")

        clip_lower, clip_upper = np.quantile(
            y,
            [self.clip_quantile, 1.0 - self.clip_quantile],
        )
        if not np.isfinite((clip_lower, clip_upper)).all():
            raise ValueError("target is too large to clip safely")
        clipped_target = np.clip(y, clip_lower, clip_upper)
        target_mean = float(clipped_target.mean())
        target_std = float(clipped_target.std())
        if not np.isfinite(target_mean) or not np.isfinite(target_std):
            raise ValueError("target is too large to standardize safely")
        target_scale = target_std if target_std >= 1e-12 else 1.0
        normalized_target = (clipped_target - target_mean) / target_scale

        samples, feature_count = normalized.shape
        if self.l2 == 0.0:
            normalized_coefficients = np.linalg.lstsq(
                normalized,
                normalized_target,
                rcond=None,
            )[0]
        else:
            covariance = normalized.T @ normalized / samples
            covariance.flat[:: feature_count + 1] += self.l2
            cross_product = normalized.T @ normalized_target / samples
            try:
                normalized_coefficients = np.linalg.solve(
                    covariance,
                    cross_product,
                )
            except np.linalg.LinAlgError:
                normalized_coefficients = np.linalg.lstsq(
                    covariance,
                    cross_product,
                    rcond=None,
                )[0]

        coefficients = normalized_coefficients * target_scale
        predictions = normalized @ coefficients + target_mean
        rmse = float(np.sqrt(np.mean(np.square(predictions - clipped_target))))
        if not np.isfinite(coefficients).all() or not np.isfinite(rmse):
            raise ValueError("training calculation produced non-finite values")

        self.mean_ = feature_mean
        self.scale_ = feature_scale
        self.coef_ = coefficients
        self.intercept_ = target_mean
        self.target_mean_ = target_mean
        self.target_std_ = target_std
        self.clip_lower_ = float(clip_lower)
        self.clip_upper_ = float(clip_upper)
        self.summary_ = RidgeFitSummary(
            samples=samples,
            features=feature_count,
            target_mean=target_mean,
            target_std=target_std,
            rmse=rmse,
            clip_lower=float(clip_lower),
            clip_upper=float(clip_upper),
        )
        return self.summary_

    def predict(self, features: np.ndarray) -> np.ndarray:
        if (
            self.mean_ is None
            or self.scale_ is None
            or self.coef_ is None
            or self.intercept_ is None
        ):
            raise RuntimeError("Model has not been fitted")
        x = np.asarray(features, dtype=float)
        if x.ndim == 1:
            x = x.reshape(1, -1)
        if x.ndim != 2 or x.shape[1] != len(self.coef_):
            raise ValueError("Prediction feature shape does not match the model")
        if not np.isfinite(x).all():
            raise ValueError("Prediction features must be finite")
        normalized = (x - self.mean_) / self.scale_
        predictions = normalized @ self.coef_ + self.intercept_
        if not np.isfinite(predictions).all():
            raise ValueError("Prediction calculation produced non-finite values")
        return predictions


def binary_auc(target: np.ndarray, scores: np.ndarray) -> float:
    y = np.asarray(target, dtype=float).reshape(-1)
    prediction = np.asarray(scores, dtype=float).reshape(-1)
    if len(y) != len(prediction) or len(y) == 0:
        return float("nan")
    positive_count = int((y == 1).sum())
    negative_count = int((y == 0).sum())
    if positive_count == 0 or negative_count == 0:
        return float("nan")

    order = np.argsort(prediction, kind="mergesort")
    sorted_scores = prediction[order]
    ranks = np.empty(len(prediction), dtype=float)
    start = 0
    while start < len(prediction):
        end = start + 1
        while end < len(prediction) and sorted_scores[end] == sorted_scores[start]:
            end += 1
        average_rank = (start + 1 + end) / 2.0
        ranks[order[start:end]] = average_rank
        start = end

    positive_rank_sum = float(ranks[y == 1].sum())
    return (
        positive_rank_sum - positive_count * (positive_count + 1) / 2
    ) / (positive_count * negative_count)
