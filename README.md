# GlobalDataEngine
GlobalDataEngine 是一个为企业级量化研究和交易系统设计的专业数据基础设施。它提供统一的API接口，支持股票、期货、加密货币、外汇等多种资产类别的历史数据和实时数据获取，具备完整的数据质量监控、自动化更新和可扩展架构。<br>
GlobalDataEngine/<br>
├── engine/<br>
│   ├── __init__.py<br>
│   ├── data_source.py      # 统一API接口层 (目前阶段)<br>
│   ├── data_manager.py     # 数据存储管理层<br>
│   ├── quality_checker.py  # 质量监控模块<br>
│   └── scheduler.py        # 自动化调度器<br>
├── databases/<br>
│   ├── models.py           # SQLAlchemy数据模型<br>
│   └── connector.py        # 数据库连接管理<br>
├── config/<br>
│   ├── settings.py         # 全局配置<br>
│   └── assets_config.yaml  # 资产类别配置<br>
├── scripts/<br>
│   ├── init_database.py    # 数据库初始化<br>
│   └── backfill_historical.py # 历史数据回填<br>
├── tests/                  # 单元测试<br>
├── logs/                   # 运行日志<br>
├── requirements.txt<br>
└── README.md<br>
