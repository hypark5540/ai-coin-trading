from __future__ import annotations

import numpy as np
import pytest

from coinpilot.model import (
    StandardizedLogisticRegression,
    StandardizedRidgeRegression,
    binary_auc,
)


def test_logistic_model_is_deterministic_and_learns_signal() -> None:
    generator = np.random.default_rng(4)
    features = generator.normal(size=(400, 3))
    target = (features[:, 0] - 0.5 * features[:, 1] > 0).astype(float)

    first = StandardizedLogisticRegression(max_iterations=400)
    second = StandardizedLogisticRegression(max_iterations=400)
    first.fit(features, target)
    second.fit(features, target)
    first_scores = first.predict_proba(features)
    second_scores = second.predict_proba(features)

    np.testing.assert_allclose(first_scores, second_scores)
    assert binary_auc(target, first_scores) > 0.98


def test_auc_handles_ties() -> None:
    target = np.array([0, 0, 1, 1], dtype=float)
    scores = np.array([0.5, 0.5, 0.5, 0.5], dtype=float)
    assert binary_auc(target, scores) == 0.5


def test_ridge_model_is_deterministic_and_learns_continuous_signal() -> None:
    generator = np.random.default_rng(11)
    features = generator.normal(size=(500, 3))
    target = (
        0.4
        + 1.7 * features[:, 0]
        - 0.8 * features[:, 1]
        + 0.25 * features[:, 2]
        + generator.normal(scale=0.01, size=len(features))
    )

    first = StandardizedRidgeRegression(l2=1e-8, clip_quantile=0.0)
    second = StandardizedRidgeRegression(l2=1e-8, clip_quantile=0.0)
    first_summary = first.fit(features, target)
    second_summary = second.fit(features, target)

    np.testing.assert_allclose(first.predict(features), second.predict(features))
    np.testing.assert_allclose(first.coef_, second.coef_)
    assert first_summary == second_summary
    assert first_summary.samples == 500
    assert first_summary.features == 3
    assert first_summary.rmse < 0.02
    assert first_summary.clip_bounds == (float(target.min()), float(target.max()))


def test_ridge_winsorizes_target_using_training_quantiles_only() -> None:
    features = np.arange(20, dtype=float).reshape(-1, 1)
    target = 1.0 + 0.5 * features[:, 0]
    target[[0, -1]] = (-1_000.0, 1_000.0)
    original_target = target.copy()
    quantile = 0.1
    expected_lower, expected_upper = np.quantile(
        target,
        [quantile, 1.0 - quantile],
    )
    winsorized = np.clip(target, expected_lower, expected_upper)

    model = StandardizedRidgeRegression(l2=0.05, clip_quantile=quantile)
    summary = model.fit(features, target)
    reference = StandardizedRidgeRegression(l2=0.05, clip_quantile=0.0)
    reference.fit(features, winsorized)

    assert summary.clip_lower == pytest.approx(expected_lower)
    assert summary.clip_upper == pytest.approx(expected_upper)
    assert summary.target_mean == pytest.approx(float(winsorized.mean()))
    assert summary.target_std == pytest.approx(float(winsorized.std()))
    np.testing.assert_allclose(
        model.predict(np.array([[5.0], [100.0]])),
        reference.predict(np.array([[5.0], [100.0]])),
    )
    assert model.predict([[100.0]])[0] > summary.clip_upper
    np.testing.assert_array_equal(target, original_target)
    assert model.clip_lower_ == summary.clip_lower
    assert model.clip_upper_ == summary.clip_upper


def test_ridge_handles_constant_features_and_target() -> None:
    features = np.ones((8, 2), dtype=float)
    target = np.full(8, 3.25)
    model = StandardizedRidgeRegression()

    summary = model.fit(features, target)

    np.testing.assert_allclose(model.predict([[1.0, 1.0], [2.0, 2.0]]), 3.25)
    np.testing.assert_allclose(model.scale_, np.ones(2))
    assert summary.target_std == 0.0
    assert summary.rmse == 0.0


@pytest.mark.parametrize(
    ("features", "target", "message"),
    [
        (np.array([1.0, 2.0]), np.array([1.0, 2.0]), "two-dimensional"),
        (np.empty((2, 0)), np.array([1.0, 2.0]), "at least one column"),
        (np.ones((2, 1)), np.array([1.0]), "matching non-zero rows"),
        (np.empty((0, 1)), np.array([]), "matching non-zero rows"),
        (
            np.array([[1.0], [np.inf]]),
            np.array([1.0, 2.0]),
            "finite",
        ),
        (
            np.array([[1.0], [2.0]]),
            np.array([1.0, np.nan]),
            "finite",
        ),
    ],
)
def test_ridge_rejects_invalid_training_data(
    features: np.ndarray,
    target: np.ndarray,
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        StandardizedRidgeRegression().fit(features, target)


@pytest.mark.parametrize(
    ("l2", "clip_quantile"),
    [
        (-0.01, 0.01),
        (np.inf, 0.01),
        (0.01, -0.01),
        (0.01, 0.5),
        (0.01, np.nan),
    ],
)
def test_ridge_rejects_invalid_hyperparameters(
    l2: float,
    clip_quantile: float,
) -> None:
    with pytest.raises(ValueError):
        StandardizedRidgeRegression(l2=l2, clip_quantile=clip_quantile)


def test_ridge_validates_prediction_state_shape_and_values() -> None:
    model = StandardizedRidgeRegression()
    with pytest.raises(RuntimeError, match="not been fitted"):
        model.predict([[1.0]])

    model.fit(np.array([[0.0], [1.0], [2.0]]), np.array([0.0, 1.0, 2.0]))
    with pytest.raises(ValueError, match="shape"):
        model.predict([[1.0, 2.0]])
    with pytest.raises(ValueError, match="finite"):
        model.predict([[np.nan]])
