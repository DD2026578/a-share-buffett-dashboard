# A股巴菲特指标实时看板

A股总市值 ÷ GDP（证券化率）实时监控：自动抓取沪深两市总市值、PE/PB 分位、ERP、涨停/跌停与集中度，支持阈值预警与深色模式。

## 本地运行
双击 启动实时看板.bat，或运行 `python buffett_server.py`，访问 http://127.0.0.1:8787/

## 数据源
- 东方财富：沪深总市值、涨停/跌停池、前十大权重
- 蛋卷：沪深300 / 中证500 的 PE、PB、分位
- 新浪：两市成交额

## GitHub Pages
在仓库 Settings → Pages 中选择 Source: GitHub Actions，工作流每 30 分钟更新数据并发布。
