# metrics.py (diff 형태가 아니라 전체 사용 가능)
from __future__ import annotations
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional, Any, Tuple
import pandas as pd


@dataclass
class Context:
    tables: Dict[str, pd.DataFrame]
    start_date: Any
    end_date: Any
    params: Dict[str, Any] = None

    def __post_init__(self):
        if self.params is None:
            self.params = {}


MetricFn = Callable[[Context, Dict[str, pd.DataFrame]], pd.DataFrame]


@dataclass(frozen=True)
class Metric:
    key: str
    title: str
    description: str

    # ✅ 성질별 그룹핑
    category: str                      # 예: "Sales", "Post-sale", "Profitability"
    subcategory: str = ""              # 예: "Revenue", "Refunds", "Margin"
    tags: Tuple[str, ...] = ()         # 예: ("kpi", "daily", "core")

    depends_on: Tuple[str, ...] = ()
    compute: Optional[MetricFn] = None
    drilldown_dims: Tuple[str, ...] = ()

    def validate(self):
        if not self.key:
            raise ValueError("Metric.key is required")
        if not self.category:
            raise ValueError(f"Metric.category is required for {self.key}")
        if self.compute is None:
            raise ValueError(f"Metric.compute is required for {self.key}")


class MetricRegistry:
    def __init__(self, name: str = "default"):
        self.name = name
        self._metrics: Dict[str, Metric] = {}

    def register(self, metric: Metric) -> None:
        metric.validate()
        if metric.key in self._metrics:
            raise ValueError(f"Metric already exists: {metric.key}")
        self._metrics[metric.key] = metric

    def get(self, key: str) -> Metric:
        if key not in self._metrics:
            raise KeyError(f"Unknown metric: {key}")
        return self._metrics[key]

    # ✅ 카테고리 목록 / 필터
    def categories(self) -> List[str]:
        return sorted(set(m.category for m in self._metrics.values()))

    def list_by_category(self, category: str, subcategory: str = "") -> List[Metric]:
        ms = [m for m in self._metrics.values() if m.category == category]
        if subcategory:
            ms = [m for m in ms if m.subcategory == subcategory]
        return sorted(ms, key=lambda x: x.key)

    def list_by_tag(self, tag: str) -> List[Metric]:
        return sorted([m for m in self._metrics.values() if tag in m.tags], key=lambda x: x.key)

    def compute_metric(self, key: str, ctx: Context) -> pd.DataFrame:
        cache: Dict[str, pd.DataFrame] = {}
        return self._compute_recursive(key, ctx, cache)

    def compute_category(self, category: str, ctx: Context, tag: str = "") -> Dict[str, pd.DataFrame]:
        """
        한 카테고리의 지표들을 한 번에 계산해서 dict로 반환.
        tag를 주면 그 tag가 붙은 지표만 계산.
        """
        cache: Dict[str, pd.DataFrame] = {}
        metrics = self.list_by_category(category)
        if tag:
            metrics = [m for m in metrics if tag in m.tags]
        out = {}
        for m in metrics:
            out[m.key] = self._compute_recursive(m.key, ctx, cache)
        return out

    def _compute_recursive(self, key: str, ctx: Context, cache: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        if key in cache:
            return cache[key]
        m = self.get(key)

        dep_results: Dict[str, pd.DataFrame] = {}
        for dep in m.depends_on:
            dep_results[dep] = self._compute_recursive(dep, ctx, cache)

        df = m.compute(ctx, dep_results)
        if "value" not in df.columns:
            raise ValueError(f"Metric {key} must return dataframe with 'value'")
        cache[key] = df
        return df


def _to_date(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s).dt.date

def _filter_by_date(df: pd.DataFrame, ts_col: str, start_date, end_date) -> pd.DataFrame:
    d = _to_date(df[ts_col])
    mask = (d >= pd.to_datetime(start_date).date()) & (d <= pd.to_datetime(end_date).date())
    out = df.loc[mask].copy()
    out["date"] = d.loc[mask]
    return out


# ---------- Metric compute fns ----------
# ---------- Metric compute fns ----------
def metric_gross_sales(ctx: Context, deps: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    # ✅ net_sales_amount 대신 gross_amount를 사용하도록 수정
    items = _filter_by_date(ctx.tables["order_items"], "order_ts", ctx.start_date, ctx.end_date)
    daily = items.groupby("date", as_index=False)["gross_amount"].sum() 
    return daily.rename(columns={"gross_amount": "value"})

def metric_refund_amount(ctx: Context, deps: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    adj = _filter_by_date(ctx.tables["adjustments"], "event_ts", ctx.start_date, ctx.end_date)
    daily = adj.groupby("date", as_index=False)["amount"].sum()
    return daily.rename(columns={"amount": "value"})

def metric_net_sales(ctx: Context, deps: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    gross = deps["gross_sales"][["date","value"]].rename(columns={"value":"gross"})
    refund = deps["refund_amount"][["date","value"]].rename(columns={"value":"refund"})
    df = gross.merge(refund, on="date", how="outer").fillna(0)
    df["value"] = df["gross"] + df["refund"]
    return df[["date","value"]].sort_values("date")

def metric_orders(ctx: Context, deps: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    orders = _filter_by_date(ctx.tables["orders"], "order_ts", ctx.start_date, ctx.end_date)
    daily = orders.groupby("date", as_index=False)["order_id"].nunique()
    return daily.rename(columns={"order_id":"value"})

def metric_coupon_cost(ctx: Context, deps: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    items = _filter_by_date(ctx.tables["order_items"], "order_ts", ctx.start_date, ctx.end_date)
    daily = items.groupby("date", as_index=False)["discount_amount"].sum()
    return daily.rename(columns={"discount_amount":"value"})

def metric_profit_proxy(ctx: Context, deps: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    net = deps["net_sales"][["date","value"]].rename(columns={"value":"net"})
    coupon = deps["coupon_cost"][["date","value"]].rename(columns={"value":"coupon"})
    df = net.merge(coupon, on="date", how="outer").fillna(0)
    df["value"] = df["net"] - df["coupon"]
    return df[["date","value"]].sort_values("date")

def metric_payment_fee(ctx: Context, deps: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    gross = deps["gross_sales"]
    # 평균 수수료율 3.3% 가정
    df = gross.copy()
    df["value"] = df["value"] * 0.033 
    return df

def build_default_registry() -> MetricRegistry:
    r = MetricRegistry(name="default_ecommerce")

    r.register(Metric(
        key="gross_sales",
        title="Gross Sales",
        description="Sum of sales per day.",
        category="Sales",
        subcategory="Revenue",
        tags=("kpi","daily","core"),
        compute=metric_gross_sales,
        drilldown_dims=("channel","influencer_id","product_id"),
    ))

    r.register(Metric(
        key="orders",
        title="Orders",
        description="Distinct orders per day.",
        category="Sales",
        subcategory="Volume",
        tags=("kpi","daily","core"),
        compute=metric_orders,
        drilldown_dims=("channel",),
    ))

    r.register(Metric(
        key="refund_amount",
        title="Refund",
        description="Sum of adjustments (negative).",
        category="Post-sale",
        subcategory="Refunds",
        tags=("kpi","daily","core"),
        compute=metric_refund_amount,
        drilldown_dims=("product_id","seller_id","reason_code"),
    ))

    r.register(Metric(
        key="net_sales",
        title="Net Sales",
        description="Gross + Refund.",
        category="Profitability",
        subcategory="Revenue",
        tags=("kpi","daily","core"),
        depends_on=("gross_sales","refund_amount"),
        compute=metric_net_sales,
        drilldown_dims=("channel","product_id"),
    ))

    r.register(Metric(
        key="coupon_cost",
        title="Coupon Cost",
        description="Sum of discount_amount per day.",
        category="Discount",
        subcategory="Coupons",
        tags=("daily",),
        compute=metric_coupon_cost,
        drilldown_dims=("coupon_id","coupon_type"),
    ))

    r.register(Metric(
        key="profit_proxy",
        title="Profit (proxy)",
        description="NetSales - CouponCost.",
        category="Profitability",
        subcategory="Profit",
        tags=("kpi","daily"),
        depends_on=("net_sales","coupon_cost"),
        compute=metric_profit_proxy,
        drilldown_dims=("channel",),
    ))

    return r
