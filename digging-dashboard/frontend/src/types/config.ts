/**
 * 配置管理相关类型定义
 */

export interface DiggingConfig {
  template_id?: number;
  region: string;
  universe: string;
  delay: number;
  decay: number;
  neutralization: string;
  instrument_type: string;
  max_trade: string;
  n_jobs: number;
  mode: string;
  use_recommended_fields: boolean;
  dataset_id?: string;
  recommended_name?: string;
  recommended_fields?: string[];
}

export interface DiggingConfigCreate {
  name: string;
  description?: string;
  region: string;
  universe: string;
  delay: number;
  decay: number;
  neutralization: string;
  instrument_type: string;
  max_trade: string;
  n_jobs: number;
  mode: string;
  use_recommended_fields: boolean;
  dataset_id?: string;
  recommended_name?: string;
  recommended_fields?: string[];
}

export interface DiggingConfigUpdate {
  name?: string;
  description?: string;
  region?: string;
  universe?: string;
  delay?: number;
  decay?: number;
  neutralization?: string;
  instrument_type?: string;
  max_trade?: string;
  n_jobs?: number;
  mode?: string;
  use_recommended_fields?: boolean;
  dataset_id?: string;
  recommended_name?: string;
  recommended_fields?: string[];
}

export interface ConfigTemplate {
  id: number;
  name: string;
  description?: string;
  config_data: DiggingConfig;
  tag_preview: string;
  created_at: string;
  updated_at: string;
  created_by: number;
}

export interface TagGenerationRequest {
  region: string;
  delay: number;
  instrument_type: string;
  universe: string;
  use_recommended_fields: boolean;
  dataset_id?: string;
  recommended_name?: string;
  step?: number;
}

export interface TagGenerationResponse {
  tag: string;
}

export interface ConfigValidationResult {
  valid: boolean;
  message: string;
  tag_preview?: string;
}

export interface FieldOptions {
  regions: string[];
  universes: string[];
  instrument_types: string[];
  max_trades: string[];
  neutralizations: string[];
  modes: string[];
}