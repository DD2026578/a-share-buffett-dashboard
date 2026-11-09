#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""核心口径函数的单元测试（纯计算，不发网络请求、不写任何文件）。

运行：python test_calcs.py
"""

import datetime
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import buffett_server as server

CST = server.CST
NOW = datetime.datetime(2025, 6, 10, 10, 30, tzinfo=CST)


def make_cfg(**kw):
    cfg = {"rf": 2.0, "gdp_ttm": 120.0, "mode": "fixed", "low": 50.0,
           "mid": 64.0, "high": 77.0, "rolling_months": 60}
    cfg.update(kw)
    return cfg


def make_hist(series=None, gdp=None):
    return {
        "gdp": {"2024": 1000000.0} if gdp is None else gdp,
        "series": [["2025-04", 55.0, 5500.0], ["2025-05", 58.0, 5800.0]]
        if series is None else series,
    }


def build_state(hist=None, cfg=None, **kw):
    args = dict(
        caps={"sh": 40.0, "sz": 18.0, "bj": 2.0}, amount=8000.0, zt=50, dt=3,
        valuation={"hs300": {"pe": 12.5, "pb": 1.4}, "zz500": {"pe": 22.0, "pb": 1.8}},
        top10={"ratio": 12.3, "names": ["贵州茅台"]},
        hist=hist if hist is not None else make_hist(),
        cfg=cfg if cfg is not None else make_cfg(),
        now=NOW, sources={"caps": "东方财富"},
    )
    args.update(kw)
    return server.build_state(**args)


class TestPctRank(unittest.TestCase):
    def test_basic(self):
        self.assertEqual(server.pct_rank([1, 2, 3, 4, 5], 3), 60.0)

    def test_none_values_filtered(self):
        self.assertEqual(server.pct_rank([1, None, 3], 2), 50.0)

    def test_empty_or_none(self):
        self.assertIsNone(server.pct_rank([], 1))
        self.assertIsNone(server.pct_rank([1, 2], None))


class TestPercentileOf(unittest.TestCase):
    def test_middle(self):
        series = [["m", 10, 0], ["m", 20, 0], ["m", 30, 0]]
        self.assertEqual(server.percentile_of(series, 20), 66.7)

    def test_extremes(self):
        series = [["m", 10, 0], ["m", 20, 0]]
        self.assertEqual(server.percentile_of(series, 5), 0)
        self.assertEqual(server.percentile_of(series, 20), 100.0)

    def test_empty(self):
        self.assertEqual(server.percentile_of([], 10), 0)


class TestRollingStats(unittest.TestCase):
    def test_mean_sigma(self):
        series = [["m", 10, 0], ["m", 20, 0], ["m", 30, 0]]
        mean, sigma = server.rolling_stats(series, 60)
        self.assertEqual(mean, 20.0)
        self.assertEqual(sigma, 8.16)  # sqrt(200/3)

    def test_window_slice(self):
        series = [["m", i * 1.0, 0] for i in range(100)]
        mean, _ = server.rolling_stats(series, 60)
        self.assertEqual(mean, 69.5)  # 仅取最后 60 个：39.0..98.0 的均值

    def test_empty(self):
        self.assertEqual(server.rolling_stats([], 60), (0, 0))


class TestPctWindows(unittest.TestCase):
    def test_windows(self):
        series = [["m", float(i), 0] for i in range(1, 41)]  # 1..40
        out = server.pct_windows(series, 20)
        self.assertEqual(out["m36"], 44.4)   # 最近 36 个为 5..40，<=20 共 16 个
        self.assertEqual(out["m60"], 50.0)
        self.assertEqual(out["m120"], 50.0)
        self.assertEqual(out["all"], 50.0)


class TestHistBand(unittest.TestCase):
    def test_band(self):
        series = [["m", float(i), 0] for i in range(1, 11)]  # 1..10
        band = server.hist_band(series)
        self.assertEqual(band, {"min": 1.0, "p20": 3.0, "p50": 6.0,
                                "p80": 9.0, "max": 10.0, "n": 10})

    def test_empty(self):
        self.assertIsNone(server.hist_band([]))


class TestThresholdBacktest(unittest.TestCase):
    def make_series(self):
        ratios = [10, 10, 80, 80, 10, 10, 10, 10, 10, 10, 10, 10]
        return [["2020-%02d" % (i + 1), float(r), float(100 + i)]
                for i, r in enumerate(ratios)]

    def test_fixed_mode(self):
        bt = server.threshold_backtest(
            self.make_series(), make_cfg(mode="fixed", low=30.0, high=70.0))
        self.assertEqual(bt["rule"], "固定阈值")
        self.assertEqual(bt["th_low"], 30.0)
        self.assertEqual(bt["th_high"], 70.0)

        # 高估事件：仅 2020-03 首次触发（连续触发不重复计）
        self.assertEqual(bt["high"]["events"],
                         [["2020-03", 80.0, 2.94, 5.88, None]])
        self.assertEqual(bt["high"]["stats"]["3"],
                         {"n": 1, "avg": 2.94, "med": 2.94, "win": 100.0})
        self.assertEqual(bt["high"]["stats"]["12"],
                         {"n": 0, "avg": None, "med": None, "win": None})

        # 低估事件：2020-01 与 2020-05 两次，最近排前
        self.assertEqual(bt["deep"]["events"],
                         [["2020-05", 10.0, 2.88, 5.77, None],
                          ["2020-01", 10.0, 3.0, 6.0, None]])
        self.assertEqual(bt["deep"]["stats"]["3"],
                         {"n": 2, "avg": 2.94, "med": 3.0, "win": 100.0})

    def test_rolling_insufficient_window(self):
        # 12 个月不足 24，滚动阈值全部无效，事件为空且回退固定阈值展示
        bt = server.threshold_backtest(
            self.make_series(), make_cfg(mode="rolling", low=30.0, high=70.0))
        self.assertEqual(bt["rule"], "滚动 60 月均值±1σ")
        self.assertEqual(bt["th_low"], 30.0)
        self.assertEqual(bt["th_high"], 70.0)
        self.assertEqual(bt["high"]["events"], [])
        self.assertEqual(bt["deep"]["stats"]["3"],
                         {"n": 0, "avg": None, "med": None, "win": None})


class TestRebuildSeriesRatios(unittest.TestCase):
    def test_recompute_and_idempotent(self):
        hist = {"gdp": {"2019": 1000.0},
                "series": [["2020-01", 999.0, 5.0], ["2020-02", 50.0, 5.0]]}
        self.assertTrue(server.rebuild_series_ratios(hist))
        self.assertEqual(hist["series"][0][1], 50.0)  # 5.0 * 10000 / 1000
        self.assertFalse(server.rebuild_series_ratios(hist))  # 再跑一遍不再变化

    def test_year_minus_two_fallback(self):
        hist = {"gdp": {"2018": 1000.0}, "series": [["2020-01", 50.0, 5.0]]}
        self.assertFalse(server.rebuild_series_ratios(hist))  # 前两年兜底，结果已一致

    def test_malformed_rows_skipped(self):
        hist = {"gdp": {"2019": 1000.0},
                "series": [["2020-01"], ["2020-02", 10.0, None]]}
        self.assertFalse(server.rebuild_series_ratios(hist))


class TestBuildState(unittest.TestCase):
    def test_ratio_and_market(self):
        snap = build_state()
        self.assertEqual(snap["cur"], 6000.0)          # (40+18+2) 万亿 × 100
        self.assertEqual(snap["ratio"], 60.0)          # 6000 × 10000 / 1000000 亿
        self.assertEqual(snap["market"]["total_mv"], 60.0)
        self.assertEqual(snap["market"]["bj_share"], 3.33)
        self.assertEqual(snap["ttm"], {"gdp": 120.0, "ratio": 50.0})
        self.assertEqual(snap["asof"], "2025-06-10")

    def test_series_same_month_replaced(self):
        hist = make_hist(series=[["2025-04", 55.0, 5500.0],
                                 ["2025-06", 99.0, 9999.0]])
        snap = build_state(hist=hist)
        self.assertEqual(len(hist["series"]), 2)
        self.assertEqual(hist["series"][-1], ["2025-06", 60.0, 6000.0])

    def test_series_new_month_appended(self):
        hist = make_hist()
        snap = build_state(hist=hist)
        self.assertEqual(len(hist["series"]), 3)
        self.assertEqual(hist["series"][-1], ["2025-06", 60.0, 6000.0])
        self.assertEqual(snap["prev_ratio"], 58.0)
        self.assertEqual(snap["delta"], 2.0)
        self.assertEqual(snap["percentile"], 100.0)  # 60 为样本内最大

    def test_series_frozen_when_fallback_source(self):
        hist = make_hist()
        build_state(hist=hist, allow_series_update=False)
        self.assertEqual(len(hist["series"]), 2)  # 历史序列不被兜底数据污染

    def test_fixed_thresholds(self):
        snap = build_state(cfg=make_cfg(mode="fixed"))
        self.assertEqual(snap["thresholds"],
                         {"low": 50.0, "mid": 64.0, "high": 77.0, "mode": "fixed"})

    def test_rolling_thresholds(self):
        # 更新后序列 = [50, 55, 60]，均值 55，σ=sqrt(50/3)≈4.08
        hist = make_hist(series=[["2025-04", 50.0, 5000.0],
                                 ["2025-05", 55.0, 5500.0],
                                 ["2025-06", 55.0, 5500.0]])
        snap = build_state(hist=hist, cfg=make_cfg(mode="rolling"))
        self.assertEqual(snap["thresholds"],
                         {"low": 50.92, "mid": 55.0, "high": 59.08, "mode": "rolling"})

    def test_erp_with_config_rf(self):
        snap = build_state()  # rf_auto 缺省 → 用配置 rf=2.0
        self.assertEqual(snap["erp"], 6.0)  # 100/12.5 - 2.0
        self.assertEqual(snap["rf"], 2.0)
        self.assertEqual(snap["rf_source"], "配置值")

    def test_erp_with_auto_rf(self):
        snap = build_state(rf_auto=2.5)
        self.assertEqual(snap["erp"], 5.5)
        self.assertEqual(snap["rf"], 2.5)
        self.assertEqual(snap["rf_source"], "10Y国债·东方财富")


class TestLevelOf(unittest.TestCase):
    def test_boundaries(self):
        th = {"low": 50, "mid": 64, "high": 77}
        self.assertEqual(server.level_of(80, th), ("high", "进入高估区"))
        self.assertEqual(server.level_of(77, th), ("high", "进入高估区"))
        self.assertEqual(server.level_of(70, th)[0], "mid")
        self.assertEqual(server.level_of(50, th)[0], "low")
        self.assertEqual(server.level_of(49.9, th)[0], "deep")


class TestBreadthOf(unittest.TestCase):
    def test_levels(self):
        b = server.breadth_of({"up": 700, "down": 200, "flat": 100})
        self.assertEqual(b["total"], 1000)
        self.assertEqual(b["pct"], 70.0)
        self.assertEqual(b["mood"], "情绪火热")
        self.assertEqual(b["level"], "high")
        self.assertEqual(b["adv_ratio"], 3.5)

    def test_zero_down(self):
        b = server.breadth_of({"up": 10, "down": 0, "flat": 0})
        self.assertIsNone(b["adv_ratio"])

    def test_empty(self):
        self.assertIsNone(server.breadth_of(None))


if __name__ == "__main__":
    unittest.main(verbosity=2)
