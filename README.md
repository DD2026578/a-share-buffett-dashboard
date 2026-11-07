# A股巴菲特指标实时看板
在线看板：https://DD2026578.github.io/a-share-buffett-dashboard/
A股总市值 ÷ GDP（证券化率）实时监控：自动抓取沪深两市总市值、PE/PB 分位、ERP、涨停/跌停与集中度，支持阈值预警与深色模式。
## 本地运行
在项目目录执行 `python buffett_server.py`。服务默认只绑定本机回环地址，不对外网开放；启动后按终端输出的地址在浏览器打开即可。
## 数据源
- 东方财富：沪深总市值、涨停/跌停池、前十大权重
- 蛋卷：沪深300 / 中证500 的 PE、PB、分位
- 新浪：两市成交额
## GitHub Pages
Pages 已启用（Source: GitHub Actions），工作流每 30 分钟更新数据并发布。
## 安全与隐私
本仓库不含个人环境信息，公开内容仅为行情数据：
- 本地启动脚本（.bat / .cmd / .ps1）含本机绝对路径与用户名，已在 .gitignore 中排除，不会入库
- 页面与数据文件不含内网地址、账号名、凭据或任何私有字段
- CI 采用最小权限（contents: write / pages: write / id-token: write），仅用于提交构建产物与发布 Pages
