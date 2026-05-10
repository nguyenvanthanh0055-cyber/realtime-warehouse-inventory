import os
from pathlib import Path
from dataclasses import dataclass

PATH_ROOT = Path(__file__).resolve().parents[2]

@dataclass
class LakeConfig:
    lake_root: Path
    checkpoint_root: Path

    @property
    def bronze_raw_inventory_events_path(self) -> str:
        return str(self.lake_root / "bronze" / "raw_inventory_events")
    
    @property
    def silver_inventory_movement_path(self) -> str:
        return str(self.lake_root / "silver" / "inventory_movements")
    
    @property
    def silver_inventory_alerts_path(self) -> str:
        return str(self.lake_root / "silver" / "inventory_alerts")
    
    @property
    def bronze_checkpoint_path(self) -> str:
        return str(self.checkpoint_root / "bronze_raw_inventory_events")

    @property
    def silver_movements_checkpoint_path(self) -> str:
        return str(self.checkpoint_root / "silver_inventory_movements")

    @property
    def silver_alerts_checkpoint_path(self) -> str:
        return str(self.checkpoint_root / "silver_inventory_alerts")
    
    @property
    def silver_sales_velocity_5m_path(self) -> str:
        return str(self.lake_root / "silver" / "sales_velocity_5m")
    
    @property
    def silver_sales_velocity_5m_checkpoint_path(self) -> str:
        return str(self.checkpoint_root / "silver_sales_velocity_5m")
    
def load_lake_config() -> LakeConfig:
    lake_root = Path(
        os.getenv("LOCAL_LAKE_ROOT", PATH_ROOT / "data" / "lake")
    )

    checkpoint_root = Path(
        os.getenv(
            "LOCAL_STREAMING_CHECKPOINT_ROOT",
            PATH_ROOT / "data" / "checkpoints"
        )
    )

    return LakeConfig(
        lake_root=lake_root,
        checkpoint_root=checkpoint_root,
    )