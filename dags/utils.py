# dags/utils.py
import json
import polars as pl
from decimal import Decimal
from pydantic import BaseModel, Field, field_validator
from typing import Optional

# ⚡ THE FINAL ARCHITECTURAL CLASS NAME:
class SchemaCreator(BaseModel):
    """
    Unified Data Gateway: Enforces schema validation contracts while 
    natively parsing and flattening incoming raw API payloads [1.12].
    """
    target_latitude: Decimal = Field(..., description="Geographic latitude mapping vector.")
    target_longitude: Decimal = Field(..., description="Geographic longitude mapping vector.")
    timezone_name: str = Field(..., min_length=2)
    timezone_offset: int = Field(...)
    temperature: float = Field(..., description="Ambient temperature measured in Celsius.")
    feels_like: float = Field(...)
    humidity: int = Field(..., ge=0, le=100, description="Relative humidity percentage.")
    cloud_cover: int = Field(..., ge=0, le=100)
    wind_speed: float = Field(..., ge=0, description="Wind speed velocity in km/h.")
    weather_code: int = Field(...)
    dew_point: float = Field(...)
    pressure: int = Field(..., ge=800, le=1100, description="Surface barometric pressure in hPa.")
    uv_index: float = Field(..., ge=0)
    visibility_meters: Optional[int] = Field(default=None, ge=0, description="Horizontal atmospheric clarity in meters.")
    observation_time: str = Field(...)
    sunrise_time: str = Field(...)
    sunset_time: str = Field(...)

    # VALUE GUARDRAIL VALIDATOR
    @field_validator('temperature', 'feels_like')
    @classmethod
    def validate_realistic_temperatures(cls, value: float) -> float:
        """Sanity check blocking corrupt sensor read spikes from hitting analytics tiers."""
        if value < -60.0 or value > 60.0:
            raise ValueError(f"❌ Structural Anomaly Detected: Unrealistic weather temperature read out ({value}°C).")
        return value

    # THE UNIFIED CONSTRUCTOR
    @classmethod
    def from_raw_payload(cls, raw_bytes: bytes) -> dict:
        """
        Parses raw API bytes into an in-memory Polars frame, normalizes fields 
        using explicit struct sub-indexing, and outputs a validated dictionary [1.12].
        """
        json_data = json.loads(raw_bytes)
        df_raw = pl.DataFrame([json_data])

        # High-performance Polars functional select expression grid with zero explodes or unnests
        df_flat = df_raw.select([
            pl.col("lat").cast(pl.Float64).alias("target_latitude"),
            pl.col("lon").cast(pl.Float64).alias("target_longitude"),
            pl.col("timezone").cast(pl.String).alias("timezone_name"),
            pl.col("timezone_offset").cast(pl.Int64),
            
            pl.col("data").list.get(0).struct.field("temp").cast(pl.Float64).alias("temperature"),
            pl.col("data").list.get(0).struct.field("feels_like").cast(pl.Float64).alias("feels_like"),
            pl.col("data").list.get(0).struct.field("humidity").cast(pl.Int64).alias("humidity"),
            pl.col("data").list.get(0).struct.field("clouds").cast(pl.Int64).alias("cloud_cover"),
            pl.col("data").list.get(0).struct.field("wind_speed").cast(pl.Float64).alias("wind_speed"),
            pl.col("data").list.get(0).struct.field("dew_point").cast(pl.Float64).alias("dew_point"),
            pl.col("data").list.get(0).struct.field("pressure").cast(pl.Int64).alias("pressure"),
            pl.col("data").list.get(0).struct.field("uvi").cast(pl.Float64).alias("uv_index"),
            pl.col("data").list.get(0).struct.field("visibility").cast(pl.Int64).alias("visibility_meters"),
            
            pl.col("data")
              .list.get(0)
              .struct.field("weather")
              .list.get(0)
              .struct.field("id")
              .cast(pl.Int64)
              .alias("weather_code"),
            
            pl.from_epoch(pl.col("data").list.get(0).struct.field("dt")).dt.strftime("%Y-%m-%d %H:%M:%S").alias("observation_time"),
            pl.from_epoch(pl.col("data").list.get(0).struct.field("sunrise")).dt.strftime("%Y-%m-%d %H:%M:%S").alias("sunrise_time"),
            pl.from_epoch(pl.col("data").list.get(0).struct.field("sunset")).dt.strftime("%Y-%m-%d %H:%M:%S").alias("sunset_time")
        ])
        
        # Unwrap row dictionary out of the list envelope index 0 array
        raw_row_dict = df_flat.to_dicts()[0]

        # Instantiates, validates, and serializes the object using Pydantic natively [1.12]
        instance = cls(**raw_row_dict)
        return instance.model_dump()
