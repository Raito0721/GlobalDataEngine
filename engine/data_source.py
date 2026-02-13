"""
GlobalDataEngine - 统一市场数据接口层
提供标准化、统一的市场数据获取接口，支持多种数据源的接入与管理。
"""
from logging import config
import os
import pandas as pd
import numpy as np
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Tuple, Union, Any
from datetime import datetime, timedelta
import logging
import pytz
import baostock as bs
import akshare as ak
import yfinance as yf
import ccxt  # 加密货币交易所统一API
from dateutil.parser import parse
from functools import lru_cache, wraps
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import sqlite3
import time

# 配置日志
logger = logging.getLogger(__name__) 
logger.setLevel(logging.INFO) 
handler = logging.StreamHandler()
log_file = os.path.abspath(__file__)
log_file = os.path.dirname(log_file)
data_path = log_file.replace('engine','data')
log_file = log_file.replace('engine','logs')
log_file_path = os.path.join(f'{log_file}', 'data_source.log')
handler1 = logging.FileHandler(log_file_path, encoding='utf-8')
formatter = logging.Formatter('%(asctime)s —— %(name)s —— %(levelname)s —— %(message)s')
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
                      end_date: Union[str, datetime], fields: List[str]) -> pd.DataFrame:
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

def get_latest_date_with_data(log_file: str, keyword: str) -> datetime:
    """获取日志相关更新的最近日期"""
    with open(log_file, 'r', encoding='utf-8') as f:
        lines = f.readlines()

    for line in reversed(lines):
        if keyword in line:
            date = line.split('——')[0] 
            date = date.split(' ')[0]
            return datetime.strptime(date, '%Y-%m-%d')
    return None

class AShareCodeManager: 
    """A股代码管理器 - 负责A股代码的获取、验证和元数据管理,使用BaoStock数据源
       数据库管理A股代码和历史数据,提供接口供AShareAdapter调用
    """

    def __init__(self, db_path: str = f'{data_path}/a_share_code.db', config: Dict = None, session: requests.Session = None, update: bool = False):
        self.config = config
        self.session = session
        self.db_path = db_path 
        if update:
            database_time = get_latest_date_with_data(log_file_path, "Initialized A-share code database")
            if database_time is None :
                self._init_database()
            self.code_datetime = get_latest_date_with_data(log_file_path, "A-share codes into database" )
            if self.code_datetime is None or (datetime.now() - self.code_datetime).days >=1:
                self._load_all_codes()

    def _init_database(self):
        """初始化A股代码数据库
        listing_date text,# 上市日期
        type text,# 证券类型(股票、指数、其他、可转债、ETF等)
        is_active boolean default 1,# 是否活跃（未退市）
        last_updated timestamp # 最后更新日期
        """

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(''' create table if not exists a_share_codes (
                            code text primary key,
                            name text,
                            full_code text,
                            exchange text,
                            type text,
                            listing_date text,
                            is_active boolean default 1,
                            last_updated timestamp
                       )
                    ''') 
    
        cursor.execute(''' create index if not exists idx_code on a_share_codes(code)
                        ''')
        cursor.execute(''' create index if not exists idx_exchange on a_share_codes(exchange)
                        ''')
        cursor.execute(''' create index if not exists idx_type on a_share_codes(type)
                       ''' )
        
        cursor.execute(''' create table if not exists a_share_code_history (
                            code text,
                            name text,
                            type text,
                            date text,
                            open text,
                            high text,
                            low text,
                            close text, volume text,
                            amount text,
                            turn text,
                            pctChg text,
                            primary key (code, date)
                          )
                        ''')
        cursor.execute(''' create index if not exists idx_code on a_share_code_history(code)
                        ''')
        cursor.execute(''' create index if not exists idx_date on a_share_code_history(date)
                        ''')
        cursor.execute(''' create index if not exists idx_type on a_share_code_history(type)
                       ''' )
        
        conn.commit()
        conn.close()
        logger.info("Initialized A-share code database")

    def _load_all_codes(self):
        """加载所有A股代码"""
        try:
            bs.login()
            rs = bs.query_all_stock()
            data_list = []
            while rs.next():
                data_list.append(rs.get_row_data()) 
            stock_info = pd.DataFrame(data_list, columns=rs.fields)
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            now = datetime.now().isoformat()
            codes_added = 0
            for _, row in stock_info.iterrows():
                code = row['code']
                name = row['code_name']
                rs = bs.query_stock_basic(code=code)
                while rs.next():
                    stock_info_basic = rs.get_row_data()
                if stock_info_basic[4] == '1': 
                    type = '股票'
                elif stock_info_basic[4] == '2':
                    type = '指数'
                elif stock_info_basic[4] == '3':
                    type = '其他'
                elif stock_info_basic[4] == '4':
                    type = '可转债'
                elif stock_info_basic[4] == '5':
                    type = 'ETF'
                exchange =  code.split('.')[0].upper()
                code1 = code.split('.')[1]  # 去掉交易所后缀
                full_code = f'{code1}.{exchange}'
                cursor.execute(''' insert or replace into a_share_codes 
                                (code, name, full_code, exchange, type,
                                 listing_date, is_active, last_updated) 
                                    values (?, ?, ?, ?, ?, ?, ?, ?)''',
                               (code1, name, full_code, exchange, type, stock_info_basic[2], stock_info_basic[5], now))
                
                cutoff = (datetime.today().date() - timedelta(days=30)).isoformat()
                cursor.execute(''' update a_share_codes set is_active = 0
                                where last_updated < ?
                               ''', (cutoff,))

                if self.code_datetime is None :
                    self.code_datetime = datetime.strptime('2010-01-01', '%Y-%m-%d')
                rs1 = bs.query_history_k_data_plus(code, 'date,open,high,low,close,volume,amount,turn,pctChg',
                                                    start_date=self.code_datetime.date().isoformat(), end_date=datetime.now().date().isoformat(), frequency='d', adjustflag='2')
                results = []
                while rs1.next():
                    row = rs1.get_row_data()
                    results.append((code, name, type, row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8]))
                cursor.executemany(''' insert or replace into a_share_code_history 
                                    (code, name, type, date, open, high, low, close, volume, amount, turn, pctChg) 
                                        values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                                   results)
                codes_added += 1
                print(codes_added)
            conn.commit()
            conn.close()
            bs.logout()
            print(f"Successfully loaded {codes_added} A-share codes")
            logger.info(f"Loaded {codes_added} A-share codes into database")

        except Exception as e:
            print(f"Error loading A-share codes: {e}")
            # 回退到内置的代码列表
            self._fallback_codes = {
                '000001.SZ': '平安银行',
                '000002.SZ': '万科A',
                '000858.SZ': '五粮液',
                '000333.SZ': '美的集团',
                '600000.SH': '浦发银行',
                '600036.SH': '招商银行',
                '600519.SH': '贵州茅台',
                '601318.SH': '中国平安',
            }

    def get_stock_name(self, symbol: str) -> Optional[str]:
        """
        统一返回
        根据股票代码，获取股票名称
        参数:
            symbol: 股票代码，如 '000001.SZ' 或 '000001' 或 'sz.000001'或 股票名字
        返回:
            股票名称,如果代码无效则返回None
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        symbol = symbol.strip().upper()
        if '.' not in symbol:
            # 可能是纯代码或股票名称
            code = symbol
            if symbol.isdigit() and len(symbol) == 6:
                # 纯代码，尝试匹配
                cursor.execute('''
                SELECT name FROM a_share_codes 
                WHERE code = ? AND is_active = 1
                ''', (code,))
                result = cursor.fetchone()
                conn.close()
                return result[0] if result else None
            else:
                # 可能是股票名称，尝试模糊匹配
                cursor.execute('''
                SELECT name FROM a_share_codes 
                WHERE name LIKE ? AND is_active = 1
                ''', (f'%{symbol}%',))
                result = cursor.fetchone()
                conn.close()
                if result:
                    return result[0]
                else:
                    return None
        else:
            # 可能是带交易所后缀的代码
            code = symbol.split('.')[0]
            if len(code) == 6 :
                cursor.execute('''
                SELECT name FROM a_share_codes 
                WHERE code = ? AND is_active = 1
                ''', (code,))
                result = cursor.fetchone()
                conn.close()
                return result[0] if result else None
            else:
                code = symbol.split('.')[1]
                cursor.execute('''  
                SELECT name FROM a_share_codes 
                WHERE code = ? AND is_active = 1
                ''', (code,))
                result = cursor.fetchone()
                conn.close()
                return result[0] if result else None
            
    def is_valid_a_share(self, symbol: str) -> Tuple[bool, str, str, str, str]:
        """
        验证是否是有效的A股代码
        
        参数:
           symbol: 资产代码，如 '000001.SZ' 或 '000001'
            
        返回:
          (是否有效, 标准代码, 股票名称, 证券类型, 上市日期)
        """
        # 清理输入
        name = self.get_stock_name(symbol)
        if name:
        # 查询数据库    
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
        
            cursor.execute('''
            SELECT is_active, full_code, name, type, listing_date FROM a_share_codes 
            WHERE (name=?) AND is_active = 1
            ''', (name,))
        
            result = cursor.fetchone()
            conn.close()
        else:
            result = None

        if result:
            return True, result[1], result[2], result[3], result[4]
        
        return False, '', '', '', ''
    
    def get_stock_data(self, code: str, fields: List[str], start_date: str, end_date: str) -> pd.DataFrame:
        """从数据库中获取股票信息
        参数:
            code: 股票代码
            fields: 需要获取的字段列表，如 ['open', 'close', 'volume']
            start_date: 起始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)

        返回:
            所查询的股票信息
        """
        name = self.get_stock_name(code)
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        codes = []
        if name:
            cursor.execute(f'''
            SELECT code, name, date, {', '.join(fields)} FROM a_share_code_history 
            WHERE name = ? AND date BETWEEN ? AND ?
            ''', (name, start_date, end_date))
    
            codes = [row for row in cursor.fetchall()]
        conn.close()
        return pd.DataFrame(codes, columns=['code', 'name', 'date'] + fields)

class AShareAdapter(DataSource):
    """""BaoStock数据源适配器 - 实现DataSource接口,提供A股数据获取功能"""
    def __init__(self, config: Dict, session: requests.Session, update: bool = False):
        self.config = config
        self.session = session
        self.code_manager = AShareCodeManager(config=config, session=session, update=update)
    
    def get_daily_data(self, symbol, start_date, end_date, fields: List[str]) -> pd.DataFrame:
        stock_info = self.code_manager.get_stock_data(symbol, fields, start_date, end_date)
        return stock_info
    
    def get_intraday_data(self, symbol, interval, start_date, end_date, **kwargs) -> pd.DataFrame:
        raise NotImplementedError("A股数据源暂不支持日内数据")
    
    def get_realtime_quote(self, symbol, **kwargs) -> Dict[str, Any]:
        raise NotImplementedError("A股数据源暂不支持实时报价")
    
    def validate_symbol(self, symbol) -> bool:
        is_valid, _, _, _, _ = self.code_manager.is_valid_a_share(symbol)
        return is_valid
    
    def get_asset_metadata(self, symbol) -> Dict[str, Any]:
        is_valid, full_code, name, type, listing_date = self.code_manager.is_valid_a_share(symbol)
        if not is_valid:
            raise SymbolValidationError(f"无效的A股代码: {symbol}")
        return {
            'full_code': full_code,
            'name': name,
            'type': type,
            'listing_date': listing_date
        }
    
class USstocksManager:
    """美股代码管理器 - 负责美股代码的获取、验证和元数据管理,使用akshare数据源
       数据库管理美股代码和历史数据,提供接口供USstocksAdapter调用
    """

    def __init__(self, db_path: str = f'{data_path}/US_stock.db', config: Dict = None, session: requests.Session = None, update: bool = False):
        self.config = config
        self.session = session
        self.db_path = db_path 
        if update
            database_time = get_latest_date_with_data(log_file_path, "Initialized US stocks code database")
            if database_time is None :
                self._init_database()
            self.code_datetime = get_latest_date_with_data(log_file_path, "US stocks codes into database" )
            if self.code_datetime is None or (datetime.now() - self.code_datetime).days >=1:
                self._load_all_codes()
    
    def _init_database(self):
        """初始化美股代码数据库
         date text,# 交易日期
         last_updated timestamp # 最后更新日期
        """

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(''' create table if not exists US_stocks_code_history (
                            code text,
                            name text,
                            date text,
                            open text,
                            high text,
                            low text,
                            close text, volume text,
                            last_updated timestamp,
                            primary key (code, date)
                          )  
                    ''')
        cursor.execute(''' create index if not exists idx_code on US_stocks_code_history(code)
                        ''')
        cursor.execute(''' create index if not exists idx_date on US_stocks_code_history(date)
                        ''')

        conn.commit()
        conn.close()
        logger.info("Initialized US stocks code database")

    def _load_all_codes(self):       
        """加载所有美股代码
        因为yfinance库容易被封IP,所以这里直接使用akshare获取热门美股数据,
        备选方案:https://www.nasdaq.com/market-activity/stocks/screener?page=1&rows_per_page=25
                手动下载美股代码
        使用time.sleep(1)来避免请求过快
        """
        now = datetime.now().isoformat()
        code_list = pd.read_csv(f'{data_path}/nasdaq.csv',encoding='utf-8')
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        code_add = 0
        for _, row in code_list.iterrows():
            code = row['Symbol']
            name = row['Name']
            try:
                # time.sleep(1)  # 避免请求过快被封IP
                stock_data = ak.stock_us_daily(symbol=code)
                records = []
                for _, rows in stock_data.iterrows():
                    records.append((code, name, str(rows['date']), rows['open'], rows['high'], rows['low'], rows['close'], rows['volume'], now))
                if not stock_data.empty:
                    cursor.executemany(''' insert or replace into US_stocks_code_history 
                                        (code, name, date, open, high, low, close, volume, last_updated) 
                                            values (?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                                    records)
                        
                    code_add += 1
                    print(code_add)
            except Exception as e:
                print(f"Error loading US stocks codes: {e}")
                continue    

        conn.commit()
        conn.close()
        logger.info(f"Loaded {code_add} US stocks codes into database")

    def get_stock_data(self, code: str, fields: List[str], start_date: str, end_date: str) -> pd.DataFrame:
        """从数据库中获取股票信息
        参数:
            code: 股票代码
            fields: 需要获取的字段列表，如 ['open', 'close', 'volume']
            start_date: 起始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)

        返回:
            所查询的股票信息
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        codes = []
        cursor.execute(f'''
        SELECT code, name, date, {', '.join(fields)} FROM US_stocks_code_history 
        WHERE code = ? AND date BETWEEN ? AND ?
        ''', (code, start_date, end_date))
    
        codes = [row for row in cursor.fetchall()]
        conn.close()
        return pd.DataFrame(codes, columns=['code', 'name', 'date'] + fields)

class UnifiedMarketAPI:
    """
    统一市场数据API - 管理多个数据源适配器
    提供统一接口供外部调用，内部根据配置选择合适的数据源适配器
    """
    
    def __init__(self, config: Dict[str, Any] = None):
        """初始化统一市场数据API
        参数:
            config: 配置字典
        """
        self.config = config or self._load_default_config()
        self.session = self._create_http_session()
        self.sources = self._initialize_data_source()
        self.asset_mapping = self._load_asset_config()
    
    def _load_default_config(self) -> Dict[str, Any]:
        """加载默认配置"""
        return {
            'api_timeout': 30,
            'max_retries': 3,
            'retry_backoff_factor': 0.3,
            'cache_size': 1024,
            'default_timezone': 'UTC',
        }
    
    def _create_http_session(self) -> requests.Session:
        """创建带有重试机制的HTTP会话"""
        session = requests.Session()
        retries = Retry(total=self.config['max_retries'], 
                        backoff_factor=self.config['retry_backoff_factor'], 
                        status_forcelist=[429, 500, 502, 503, 504])
        adapter = HTTPAdapter(max_retries=retries)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        return session
    
    def _initialize_data_source(self) -> Dict[str, DataSource]:
        """初始化数据源适配器"""
        sources = {'A_Share': AShareAdapter(config=self.config, session=self.session),
        #'美股': YahooFinanceAdapter(config=self.config, session=self.session)
        #'港股': HKEXAdapter(config=self.config, session=self.session),
        #'期货': TushareAdapter(config=self.config, session=self.session),
       # '加密货币': CryptoCompareAdapter(config=self.config, session=self.session),
       # '外汇': ForexAdapter(config=self.config, session=self.session),
            #'债券': BondAdapter(config=self.config, session=self.session)
        }
        return sources
    
    def _load_asset_config(self) -> Dict[str,Tuple[str,str]]:
        """加载资产-数据源映射配置"""
        # 实际应用中应从配置文件或数据库加载
        return {
            # A股
            '000001.SZ': ('A股', 'stock'),
            '600000.SH': ('A股', 'stock'),
            # 美股
            'AAPL': ('美股', 'stock'),
            'MSFT': ('美股', 'stock'),
            # 港股
            '00700.HK': ('港股', 'stock'),
            # 期货
            'CL=F': ('期货', 'future'),
            'GC=F': ('期货', 'future'),
            # 加密货币
            'BTC-USD': ('加密货币', 'crypto'),
            'ETH-USD': ('加密货币', 'crypto'),
            # 外汇
            'USD/CNY': ('外汇', 'forex'),
            'EUR/USD': ('外汇', 'forex'),
            # 债券
            'US10Y': ('债券', 'bond')
        }
    @lru_cache(maxsize=1024)
    def get_data(self, symbol: str,
                 start_date: Union[str, datetime] = None,
                 end_date: Union[str, datetime] = None,
                 frequency: str = 'daily',
                 interval: str = None,
                 **kwargs) -> pd.DataFrame:
        """统一数据获取接口

        参数:
            symbol: 资产代码
            start_date: 开始日期/时间
            end_date: 结束日期/时间
            frequency: 数据频率 ['daily', 'intraday']
            interval: 当frequency='intraday'时有效 [1m, 5m, 15m, 30m, 1h, 4h]
            kwargs: 数据源特定参数
            
        返回:
            标准化的DataFrame
            
        异常:
            SymbolValidationError: 无效的资产代码
            DataSourceError: 数据获取失败
    
        try:
            #验证资产代码
            if symbol not in self.asset_mapping:
            """

AShareCodeManager()._load_all_codes()
