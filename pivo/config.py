"""
PIVO Configuration Management
"""
import os
from dataclasses import dataclass
from dotenv import load_dotenv


@dataclass
class Config:
    """Configuration for PIVO services and connections."""
    
    # Gemini
    gemini_api_key: str
    model: str = "gemini-2.0-flash"
    
    # HDFS
    hdfs_host: str = "localhost"
    hdfs_port: int = 9000
    hdfs_web_port: int = 9870
    
    # Hive
    hive_host: str = "localhost"
    hive_port: int = 10000
    
    # Kafka
    kafka_bootstrap_servers: str = "localhost:9092"
    
    # Spark
    spark_master: str = "spark://localhost:7077"
    
    @classmethod
    def from_env(cls) -> "Config":
        """Load configuration from environment variables."""
        load_dotenv()
        
        # Resolve API Key (Prefer GOOGLE_API_KEY for new SDK)
        api_key = os.getenv("GOOGLE_API_KEY") or os.getenv("GEMINI_API_KEY")
        
        # Suppress SDK warning "Both GOOGLE_API_KEY and GEMINI_API_KEY are set"
        if "GOOGLE_API_KEY" in os.environ and "GEMINI_API_KEY" in os.environ:
            del os.environ["GEMINI_API_KEY"]
            
        if not api_key:
            raise ValueError("GOOGLE_API_KEY or GEMINI_API_KEY environment variable is required")
        
        return cls(
            gemini_api_key=api_key,
            model=os.getenv("PIVO_MODEL", "gemini-2.0-flash"),
            hdfs_host=os.getenv("HDFS_HOST", "localhost"),
            hdfs_port=int(os.getenv("HDFS_PORT", "9000")),
            hive_host=os.getenv("HIVE_HOST", "localhost"),
            hive_port=int(os.getenv("HIVE_PORT", "10000")),
            kafka_bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            spark_master=os.getenv("SPARK_MASTER", "spark://localhost:7077"),
        )
    
    @property
    def hdfs_url(self) -> str:
        """HDFS WebHDFS URL for HTTP access."""
        return f"http://{self.hdfs_host}:{self.hdfs_web_port}"
    
    @property
    def hdfs_namenode_url(self) -> str:
        """HDFS NameNode URL for native access."""
        return f"hdfs://{self.hdfs_host}:{self.hdfs_port}"
