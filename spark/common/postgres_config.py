import os
from dataclasses import dataclass

@dataclass
class PostgresConfig:
    host: str
    port: str
    db: str
    user: str
    password: str
    
    @property
    def dsn(self) -> str:
        return(
            f"host={self.host} "
            f"port={self.port} "
            f"dbname={self.db} "
            f"user={self.user} "
            f"password={self.password}"
        )

def load_postgres_config() -> PostgresConfig:
    return PostgresConfig(
        host=os.getenv("PG_HOST", "localhost"),
        port=os.getenv("PG_PORT", "5432"),
        db=os.getenv("POSTGRES_DB", "inventory_db"),
        user=os.getenv("POSTGRES_USER", "inventory_user"),
        password=os.getenv("POSTGRES_PASSWORD", "inventory_password")
    )