# GlobalDataEngine
GlobalDataEngine 是一个为企业级量化研究和交易系统设计的专业数据基础设施。它提供统一的API接口，支持股票、期货、加密货币、外汇等多种资产类别的历史数据和实时数据获取，具备完整的数据质量监控、自动化更新和可扩展架构。
GlobalDataEngine/
├── engine/
│   ├── __init__.py
│   ├── data_source.py      # 统一API接口层 (目前阶段)
│   ├── data_manager.py     # 数据存储管理层
│   ├── quality_checker.py  # 质量监控模块
│   └── scheduler.py        # 自动化调度器
├── databases/
│   ├── models.py           # SQLAlchemy数据模型
│   └── connector.py        # 数据库连接管理
├── config/
│   ├── settings.py         # 全局配置
│   └── assets_config.yaml  # 资产类别配置
├── scripts/
│   ├── init_database.py    # 数据库初始化
│   └── backfill_historical.py # 历史数据回填
├── tests/                  # 单元测试
├── logs/                   # 运行日志
├── requirements.txt
└── README.md
