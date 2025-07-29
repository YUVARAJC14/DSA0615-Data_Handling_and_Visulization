"""
Configuration module for database and PySpark settings.
"""

import os
from configparser import ConfigParser


class Config:
    """Configuration class for the Smart Expiry Tracker."""
    
    def __init__(self):
        self.config = ConfigParser()
        self._setup_defaults()
    
    def _setup_defaults(self):
        """Setup default configuration values."""
        # Database configuration
        self.DB_HOST = os.getenv('DB_HOST', 'localhost')
        self.DB_PORT = os.getenv('DB_PORT', '5432')
        self.DB_NAME = os.getenv('DB_NAME', 'smart_expiry_tracker')
        self.DB_USER = os.getenv('DB_USER', 'postgres')
        self.DB_PASSWORD = os.getenv('DB_PASSWORD', 'postgres')
        
        # PySpark configuration
        self.SPARK_APP_NAME = "SmartExpiryTracker"
        self.SPARK_MASTER = os.getenv('SPARK_MASTER', 'local[*]')
        
        # File paths
        self.BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        self.DATA_DIR = os.path.join(self.BASE_DIR, 'data')
        self.BRONZE_DIR = os.path.join(self.DATA_DIR, 'bronze')
        self.SILVER_DIR = os.path.join(self.DATA_DIR, 'silver')
        self.GOLD_DIR = os.path.join(self.DATA_DIR, 'gold')
        self.SAMPLE_DATA_DIR = os.path.join(self.BASE_DIR, 'sample_data')
        
        # Alert thresholds
        self.EXPIRY_WARNING_DAYS = 7
        self.EXPIRY_CRITICAL_DAYS = 3
        
    @property
    def database_url(self):
        """Get database connection URL."""
        return f"postgresql://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"
    
    @property
    def spark_config(self):
        """Get Spark configuration dictionary."""
        return {
            "spark.app.name": self.SPARK_APP_NAME,
            "spark.master": self.SPARK_MASTER,
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true"
        }


# Global configuration instance
config = Config()