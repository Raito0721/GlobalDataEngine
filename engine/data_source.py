"""
GlobalDataEngine - 统一市场数据接口层
提供标准化、统一的市场数据获取接口，支持多种数据源的接入与管理。
"""

import logging
import os
#import pandas as pd
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Tuple, Union, Any
from datetime import datetime, timedelta

# 配置日志
logger = logging.getLogger(__name__) 
logger.setLevel(logging.INFO) 
handler = logging.StreamHandler()
log_file=os.path.abspath(__file__)
log_file=os.path.dirname(log_file)
log_file=log_file.replace('engine','logs')
log_file=os.path.join(f'{log_file}','data_source.log')
handler1= logging.FileHandler(log_file,encoding='utf-8')
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
handler1.setFormatter(formatter)
logger.addHandler(handler)
logger.addHandler(handler1)

# 自定义异常类
class DataSourceError(Exception):
    """数据源异常基类"""
    pass

class SymbolValidationError(DataSourceError):
    """资产代码验证失败"""
    pass

class DataRetrievalError(DataSourceError):
    """数据获取失败"""
    pass

class DataStandardizationError(DataSourceError):
    """数据标准化失败"""
    pass

class RateLimitExceededError(DataSourceError):
    """API速率限制"""
    pass

class DataSource(ABC):
    """
    数据源抽象基类 - 定义统一数据获取接口
    所有具体数据源适配器必须实现此接口
    """
    
    @abstractmethod 
    def get_daily_data(self, symbol: str, start_date: Union[str, datetime], 
                      end_date: Union[str, datetime], **kwargs) -> pd.DataFrame:
        """
        获取历史日线数据
        
        参数:
            symbol: 资产代码
            start_date: 开始日期 (YYYY-MM-DD 或 datetime对象)
            end_date: 结束日期 (YYYY-MM-DD 或 datetime对象)
            kwargs: 数据源特定参数
            
        返回:
            包含标准化行情数据的DataFrame
        """
        pass
    
    @abstractmethod
    def get_intraday_data(self, symbol: str, interval: str = '1h',
                         start_date: Union[str, datetime] = None,
                         end_date: Union[str, datetime] = None,
                         **kwargs) -> pd.DataFrame:
        """
        获取日内数据
        
        参数:
            symbol: 资产代码
            interval: 时间间隔 (1m, 5m, 15m, 30m, 1h, 4h, 1d)
            start_date: 开始日期时间
            end_date: 结束日期时间
            kwargs: 数据源特定参数
            
        返回:
            包含标准化日内数据的DataFrame
        """
        pass
    
    @abstractmethod
    def get_realtime_quote(self, symbol: str, **kwargs) -> Dict[str, Any]:
        """
        获取实时报价
        
        参数:
            symbol: 资产代码
            
        返回:
            包含实时报价的字典
        """
        pass
    
    @abstractmethod
    def validate_symbol(self, symbol: str) -> bool:
        """
        验证资产代码格式是否有效
        
        参数:
            symbol: 资产代码
            
        返回:
            是否有效
        """
        pass
    
    @abstractmethod
    def get_asset_metadata(self, symbol: str) -> Dict[str, Any]:
        """
        获取资产元数据
        
        参数:
            symbol: 资产代码
            
        返回:
            包含资产元数据的字典
        """
        pass
