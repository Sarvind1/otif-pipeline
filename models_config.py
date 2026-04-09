"""
Data Models & Configuration Management
=====================================

Pydantic models for data validation and configuration handling for the TAT Calculator system.
"""

import json
import logging
from typing import Dict, List, Optional, Union
from pathlib import Path
from pydantic import BaseModel, Field, validator

logger = logging.getLogger(__name__)


class ProcessFlow(BaseModel):
    """Process flow metadata for a stage"""
    critical_path: bool
    parallel_processes: List[str] = Field(default_factory=list)
    handoff_points: List[str] = Field(default_factory=list)
    process_type: str
    team_owner: str


class FallbackCalculation(BaseModel):
    """Fallback calculation configuration"""
    expression: str


class StageConfig(BaseModel):
    """Configuration for a single stage"""
    name: str
    actual_timestamp: Optional[str] = None
    preceding_stage: Optional[Union[str, List[str]]] = None
    process_flow: ProcessFlow
    fallback_calculation: FallbackCalculation
    lead_time: str = Field(description="Lead time in days")


class StagesConfig(BaseModel):
    """Complete stages configuration"""
    stages: Dict[str, StageConfig]
    
    @validator('stages')
    def validate_stage_ids(cls, v):
        """Ensure all stage IDs are valid"""
        for stage_id in v.keys():
            if not stage_id.strip():
                raise ValueError("Stage ID cannot be empty")
        return v


def load_config(config_path: str) -> StagesConfig:
    """Load and validate configuration from JSON file"""
    try:
        with open(config_path, 'r') as f:
            config_data = json.load(f)
        return StagesConfig(**config_data)
    except FileNotFoundError:
        logger.error(f"Configuration file not found: {config_path}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in configuration file: {e}")
        raise
    except Exception as e:
        logger.error(f"Error loading configuration: {e}")
        raise


def validate_config(config: StagesConfig):
    """Validate configuration for circular dependencies"""
    # Build dependency graph
    graph = {}
    for stage_id, stage in config.stages.items():
        graph[stage_id] = stage.preceding_stage or []
    
    # Check for cycles using DFS
    visited = set()
    rec_stack = set()
    
    def has_cycle(node):
        if node in rec_stack:
            return True
        if node in visited:
            return False
            
        visited.add(node)
        rec_stack.add(node)
        
        for neighbor in graph.get(node, []):
            if neighbor in graph and has_cycle(neighbor):
                return True
        
        rec_stack.remove(node)
        return False
    
    for stage_id in graph:
        if stage_id not in visited:
            if has_cycle(stage_id):
                raise ValueError(f"Circular dependency detected involving stage {stage_id}")
